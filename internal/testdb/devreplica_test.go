package testdb

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/benchub/rotten/internal/pgss"
)

// TestDevObservedReplicaReplaysFromPrimary runs dev/docker-compose.yaml's
// observed-postgres and observed-replica, as compose defines them, on a
// primary whose init scripts never created a replication role (as on an
// existing dev volume). The replica must bootstrap itself, stay in recovery,
// replay the primary's writes, keep doing so after a restart, and serve
// pg_stat_statements and the min/max reset to rotten_observer. Each dev
// worker config's sanity check passes on its own server and fails on the
// other.
func TestDevObservedReplicaReplaysFromPrimary(t *testing.T) {
	ctx := context.Background()
	pair := StartDevObservedPair(t, func(primary *DB) {
		rows := primary.QueryInContainer(t, "postgres", `select count(*) from pg_roles where rolname = 'replicator'`)
		if rows[0][0] != "0" {
			t.Fatalf("the primary already has a replicator role before the replica starts; the test wouldn't cover an existing volume")
		}
	})
	primary := pair.Primary.Connect(t)
	replica := pair.Replica.Connect(t)

	var inRecovery bool
	if err := replica.QueryRow(ctx, `select pg_is_in_recovery()`).Scan(&inRecovery); err != nil || !inRecovery {
		t.Fatalf("replica pg_is_in_recovery() = %v, %v; want true", inRecovery, err)
	}
	if err := primary.QueryRow(ctx, `select pg_is_in_recovery()`).Scan(&inRecovery); err != nil || inRecovery {
		t.Fatalf("primary pg_is_in_recovery() = %v, %v; want false", inRecovery, err)
	}
	var slotActive bool
	if err := primary.QueryRow(ctx, `select active from pg_replication_slots where slot_name = 'observed_replica'`).Scan(&slotActive); err != nil || !slotActive {
		t.Fatalf("primary's observed_replica slot active = %v, %v; want true", slotActive, err)
	}

	if _, err := primary.Exec(ctx, `create table replay_check (n int); insert into replay_check values (1)`); err != nil {
		t.Fatal(err)
	}
	waitForReplay(t, replica, 1)
	if _, err := replica.Exec(ctx, `insert into replay_check values (2)`); err == nil {
		t.Fatal("the replica accepted a write")
	}

	// rotten_observer, from observed-init.sql, reads pg_stat_statements on
	// the replica and resets min/max there: the reset is shared memory only,
	// so a standby allows it.
	u, err := url.Parse(pair.Replica.DSN)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword("rotten_observer", "rotten_observer")
	obs, err := pgx.Connect(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer obs.Close(ctx)
	for range 3 {
		if _, err := replica.Exec(ctx, `select count(*) from replay_check where n > 0`); err != nil {
			t.Fatal(err)
		}
	}
	const entry = `select minmax_stats_since from pg_stat_statements where query like 'select count(*) from replay_check%'`
	var before, after time.Time
	if err := obs.QueryRow(ctx, entry).Scan(&before); err != nil {
		t.Fatalf("pg_stat_statements on the replica as rotten_observer: %v", err)
	}
	if err := pgss.NewReader(obs).MinmaxReset(ctx, "rotten"); err != nil {
		t.Fatalf("min/max reset on the replica: %v", err)
	}
	if err := obs.QueryRow(ctx, entry).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if !after.After(before) {
		t.Fatalf("minmax_stats_since %v after the reset, want later than %v", after, before)
	}

	// Each worker's sanity check passes on its own server only, so a
	// mis-pointed worker exits.
	for _, tc := range []struct {
		config     string
		own, other *pgx.Conn
		ownName    string
		otherName  string
	}{
		{"worker.json", primary, replica, "primary", "replica"},
		{"worker-replica.json", replica, primary, "replica", "primary"},
	} {
		check := devWorkerSanityCheck(t, tc.config)
		var ok bool
		if err := tc.own.QueryRow(ctx, check).Scan(&ok); err != nil || !ok {
			t.Errorf("dev/%s sanity check %q on the %s = %v, %v; want true", tc.config, check, tc.ownName, ok, err)
		}
		if err := tc.other.QueryRow(ctx, check).Scan(&ok); err != nil || ok {
			t.Errorf("dev/%s sanity check %q on the %s = %v, %v; want false", tc.config, check, tc.otherName, ok, err)
		}
	}

	// A restart keeps the existing data directory and resumes streaming.
	pair.Replica.Restart(t)
	replica = pair.Replica.Connect(t)
	if err := replica.QueryRow(ctx, `select pg_is_in_recovery()`).Scan(&inRecovery); err != nil || !inRecovery {
		t.Fatalf("restarted replica pg_is_in_recovery() = %v, %v; want true", inRecovery, err)
	}
	if _, err := primary.Exec(ctx, `insert into replay_check values (3)`); err != nil {
		t.Fatal(err)
	}
	waitForReplay(t, replica, 2)
	logs := pair.ReplicaLogs(t)
	if n := strings.Count(logs, "observed-replica: cloning"); n != 1 {
		t.Fatalf("replica cloned the primary %d times across a restart, want once; logs:\n%s", n, logs)
	}
}

func waitForReplay(t *testing.T, replica *pgx.Conn, want int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var n int
		err := replica.QueryRow(context.Background(), `select count(*) from replay_check`).Scan(&n)
		if err == nil && n == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("replica has %d replay_check rows (%v), want %d", n, err, want)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func devWorkerSanityCheck(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(RepoRoot(), "dev", name))
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct{ SanityCheck string }
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.SanityCheck == "" {
		t.Fatalf("dev/%s has no SanityCheck", name)
	}
	return cfg.SanityCheck
}

// TestDevObservedReplicaReusesAnUnusedSlot covers a bootstrap interrupted
// after it created the slot and before pg_basebackup used it: the slot
// exists but has reserved no WAL, so its wal_status is NULL. The replica
// must reuse it, clone, and replay.
func TestDevObservedReplicaReusesAnUnusedSlot(t *testing.T) {
	ctx := context.Background()
	pair := StartDevObservedPair(t, func(primary *DB) {
		rows := primary.QueryInContainer(t, "postgres",
			`select pg_create_physical_replication_slot('observed_replica') is not null, (select wal_status is null from pg_replication_slots where slot_name = 'observed_replica')`)
		if rows[0][1] != "t" {
			t.Fatalf("an unused slot's wal_status isn't NULL (%v); the test wouldn't cover the interrupted bootstrap", rows)
		}
	})
	primary := pair.Primary.Connect(t)
	replica := pair.Replica.Connect(t)

	var active bool
	if err := primary.QueryRow(ctx, `select active from pg_replication_slots where slot_name = 'observed_replica'`).Scan(&active); err != nil || !active {
		t.Fatalf("primary's observed_replica slot active = %v, %v; want true", active, err)
	}
	if _, err := primary.Exec(ctx, `create table replay_check (n int); insert into replay_check values (1)`); err != nil {
		t.Fatal(err)
	}
	waitForReplay(t, replica, 1)
	if n := strings.Count(pair.ReplicaLogs(t), "observed-replica: cloning"); n != 1 {
		t.Fatalf("replica cloned %d times; want 1", n)
	}
}

// TestDevObservedReplicaWaitsForAnActiveSlot covers a re-clone while the
// primary still has the slot in use, as after an interrupted backup until
// the primary notices the dead WAL sender. Another connection holds the slot
// for a few seconds; the replica must wait for it, then clone and replay.
func TestDevObservedReplicaWaitsForAnActiveSlot(t *testing.T) {
	ctx := context.Background()
	held := make(chan error, 1)
	pair := StartDevObservedPair(t, func(primary *DB) {
		primary.QueryInContainer(t, "postgres", `select pg_create_physical_replication_slot('observed_replica')`)
		go func() {
			_, _, err := primary.c.Exec(ctx, []string{"sh", "-c",
				"mkdir -p /tmp/held-wal && chown postgres /tmp/held-wal && exec gosu postgres timeout 8 pg_receivewal -h /var/run/postgresql -U postgres -S observed_replica -D /tmp/held-wal --no-loop"})
			held <- err
		}()
		deadline := time.Now().Add(15 * time.Second)
		for {
			rows := primary.QueryInContainer(t, "postgres", `select active from pg_replication_slots where slot_name = 'observed_replica'`)
			if rows[0][0] == "t" {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("pg_receivewal never made the slot active")
			}
			time.Sleep(100 * time.Millisecond)
		}
	})
	if err := <-held; err != nil {
		t.Fatalf("slot holder: %v", err)
	}
	primary := pair.Primary.Connect(t)
	replica := pair.Replica.Connect(t)

	var pid *int
	if err := primary.QueryRow(ctx, `select active_pid from pg_replication_slots where slot_name = 'observed_replica'`).Scan(&pid); err != nil || pid == nil {
		t.Fatalf("primary's observed_replica slot active_pid = %v, %v; want the replica's", pid, err)
	}
	var app string
	if err := primary.QueryRow(ctx, `select application_name from pg_stat_replication where pid = $1`, *pid).Scan(&app); err != nil || app != "observed-replica" {
		t.Fatalf("slot held by %q, %v; want observed-replica", app, err)
	}
	if _, err := primary.Exec(ctx, `create table replay_check (n int); insert into replay_check values (1)`); err != nil {
		t.Fatal(err)
	}
	waitForReplay(t, replica, 1)
	if logs := pair.ReplicaLogs(t); !strings.Contains(logs, "observed-replica: slot observed_replica is in use") {
		t.Fatalf("replica didn't log waiting for the slot:\n%s", logs)
	}
}
