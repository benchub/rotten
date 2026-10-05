#!/bin/sh
# observed-replica's entrypoint (dev/docker-compose.yaml): make this
# container a streaming replica of observed-postgres, then hand over to the
# postgres image's own entrypoint.
#
# On every start it connects to the primary as postgres and, idempotently,
# creates the replicator role and the replication slot. The primary's init
# scripts don't rerun on an existing volume, so this is where they come
# from. It clones the primary with pg_basebackup when there's no data
# directory yet, or when the existing one can't resume streaming: the slot
# was missing or lost its WAL, the data came from another primary (for
# example after the primary's volume was removed), or it's no longer a
# standby. The clone goes to a scratch directory first, so an interrupted one
# is redone next start.
set -eu

primary=${REPLICA_PRIMARY_HOST:?}
slot=${REPLICA_SLOT:-observed_replica}
data=${PGDATA:?}
export PGPASSWORD="${REPLICA_PRIMARY_PASSWORD:?}"

until pg_isready -q -h "$primary" -U postgres -d postgres; do
	echo "observed-replica: waiting for $primary"
	sleep 1
done

on_primary() { psql -X -q -At -v ON_ERROR_STOP=1 -h "$primary" -U postgres -d postgres "$@"; }

on_primary <<'SQL'
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'replicator') THEN
    CREATE ROLE replicator REPLICATION LOGIN PASSWORD 'replicator';
  END IF;
END $$;
SQL

clone=
# missing: no slot. unreserved: the slot exists but has never reserved WAL
# (wal_status is NULL), as when a bootstrap stopped between creating it and
# pg_basebackup using it; reuse it, but clone, since no WAL was kept for us.
slot_state=$(on_primary -v slot="$slot" <<'SQL'
SELECT coalesce((SELECT coalesce(wal_status, 'unreserved') FROM pg_replication_slots WHERE slot_name = :'slot'), 'missing');
SQL
)
case $slot_state in
lost)
	echo "observed-replica: slot $slot lost its WAL; recreating it"
	echo "SELECT pg_drop_replication_slot(:'slot');" | on_primary -v slot="$slot" >/dev/null
	slot_state=missing
	clone=yes
	;;
missing | unreserved)
	clone=yes
	;;
esac
if [ "$slot_state" = missing ]; then
	echo "SELECT pg_create_physical_replication_slot(:'slot');" | on_primary -v slot="$slot" >/dev/null
fi

if [ ! -s "$data/PG_VERSION" ] || [ ! -f "$data/standby.signal" ]; then
	clone=yes
else
	primary_id=$(on_primary -c 'SELECT system_identifier FROM pg_control_system()')
	local_id=$(pg_controldata "$data" | sed -n 's/^Database system identifier: *//p')
	if [ "$primary_id" != "$local_id" ]; then
		echo "observed-replica: data directory is from another primary ($local_id, not $primary_id)"
		clone=yes
	fi
fi

# slot_holder prints the PID of the primary's process using the slot, if any.
slot_holder() {
	echo "SELECT coalesce((SELECT active_pid::text FROM pg_replication_slots WHERE slot_name = :'slot' AND active), '');" |
		on_primary -v slot="$slot"
}

# wait_for_slot waits up to 60 seconds for the slot to be free. After an
# interrupted clone the primary can keep it active until it notices its WAL
# sender's client is gone, and a slot serves one connection at a time.
wait_for_slot() {
	waited=0
	while holder=$(slot_holder) && [ -n "$holder" ]; do
		if [ "$waited" -ge 60 ]; then
			echo "observed-replica: slot $slot is still in use by PID $holder on $primary after ${waited}s; giving up." \
				"See pg_stat_replication there, or pg_terminate_backend($holder)."
			exit 1
		fi
		if [ $((waited % 5)) -eq 0 ]; then
			echo "observed-replica: slot $slot is in use by PID $holder on $primary; waiting (${waited}s of 60s)"
		fi
		sleep 1
		waited=$((waited + 1))
	done
}

if [ -n "$clone" ]; then
	echo "observed-replica: cloning $primary into $data"
	rm -rf "$data"
	attempt=1
	while :; do
		wait_for_slot
		rm -rf "$data.new"
		mkdir -p "$data.new"
		chown postgres:postgres "$(dirname "$data")" "$data.new"
		chmod 700 "$data.new"
		# Retried, since another client can take the slot between the check
		# and pg_basebackup.
		if PGPASSWORD=replicator gosu postgres pg_basebackup -h "$primary" -U replicator \
			-D "$data.new" -X stream -S "$slot" -R --checkpoint=fast; then
			break
		fi
		if [ "$attempt" -ge 5 ]; then
			echo "observed-replica: pg_basebackup failed $attempt times; giving up"
			exit 1
		fi
		echo "observed-replica: pg_basebackup failed (attempt $attempt of 5); retrying in 5s"
		attempt=$((attempt + 1))
		sleep 5
	done
	mv "$data.new" "$data"
fi

exec docker-entrypoint.sh "$@"
