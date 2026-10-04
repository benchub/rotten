# The rotten database

The rotten database holds everything that workers harvest, plus pass keys and
UI users. It runs on Postgres 18 with
[pg_partman](https://github.com/pgpartman/pg_partman) 5. `rotten-server
migrate` creates and upgrades the schema. Nothing else runs migrations, and
the UI never does.

This page covers:

1. [Install Postgres 18 and pg_partman](#1-install-postgres-18-and-pg_partman).
2. [Create the roles](#2-create-the-roles).
3. [Run the migrations](#3-run-the-migrations).
4. [Set up partition maintenance and retention](#4-partition-maintenance-and-retention).
5. [What each role may do](#roles-and-what-each-may-do).

`dev/rotten-db-init.sql` is a working example of steps 1 and 2. The dev stack
runs it.

## 1. Install Postgres 18 and pg_partman

Install pg_partman 5 on the rotten database server. For example, on Debian
or Ubuntu with the PGDG repository, install the `postgresql-18-partman`
package. `docker/rotten-db.Dockerfile` does exactly that for the tests.

Then create a database named `rotten`, and create the extension in it in the
`public` schema. The migrations call `public.create_parent()` and write
`public.part_config`, so the schema matters:

```sql
CREATE DATABASE rotten;
\c rotten
CREATE EXTENSION pg_partman SCHEMA public;
```

## 2. Create the roles

The migrations grant privileges, but they don't create roles. As a superuser,
create these four login roles and make `rotten_owner` the owner of the
database. Use your own passwords or another auth method:

```sql
CREATE ROLE rotten_owner LOGIN PASSWORD '...';
CREATE ROLE rotten_ingest LOGIN PASSWORD '...';
CREATE ROLE rotten_ui LOGIN PASSWORD '...';
CREATE ROLE rotten_readonly LOGIN PASSWORD '...';

ALTER DATABASE rotten OWNER TO rotten_owner;
```

`rotten_owner` creates and maintains the partitioned tables through
pg_partman, so it needs pg_partman's non-superuser privileges. Grant them in
the `rotten` database after `CREATE EXTENSION`:

```sql
GRANT ALL ON ALL TABLES IN SCHEMA public TO rotten_owner;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO rotten_owner;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO rotten_owner;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA public TO rotten_owner;
```

If `rotten_ingest`, `rotten_ui` or `rotten_readonly` is missing, `migrate`
stops with `role "..." doesn't exist; an operator must create it before
migrate runs`. `rotten_owner` doesn't need `CREATEROLE` or superuser.

## 3. Run the migrations

Build `rotten-server` (see [building.md](building.md)) and run `migrate` as
`rotten_owner`. The DSN comes from `-dsn` or, if that's not set,
`ROTTEN_OWNER_DSN`:

```sh
ROTTEN_OWNER_DSN='postgres://rotten_owner@db.example.com/rotten' rotten-server migrate
```

```sh
rotten-server migrate -dsn 'postgres://rotten_owner@db.example.com/rotten' -retention 45d
```

`migrate` applies only the migrations that are new, so it's safe to run on
every deploy. After every run it reapplies the grants in
`migrations/permissions.sql` in one transaction. Those grants start from
nothing and add exactly what each role needs, so a privilege granted by hand
to an application role is revoked at the next `migrate`.

**Migration 0008 blocks ingest while it runs.** It builds the
`events_fingerprint_window` index with a plain `CREATE INDEX`, which holds a
`SHARE` lock on `rotten.events` and all its partitions. Reads keep working,
but the server's inserts wait until the index is built. In testing, that
took about 3 seconds for every 10 million events. Run it at a quiet time.
Workers keep their harvests in their outbox while the server waits, and
resend them afterwards. See `docs/perf.md` for the measurements.

**Migration 0009 stops if two users share an OIDC identity.** It adds
`users_provider_uid_key`, a unique index on `rotten.users (provider,
provider_uid)` for rows with a `provider_uid`. If any pair already belongs to
more than one user, it fails with `rotten.users has N (provider,
provider_uid) pair(s) shared by more than one user` and changes nothing. It
doesn't merge them for you, because the row that keeps the identity decides
whose role and history that person gets. Find them as `rotten_owner`:

```sql
SELECT provider, provider_uid, array_agg(id ORDER BY id)
  FROM rotten.users
 WHERE provider_uid IS NOT NULL
 GROUP BY 1, 2
HAVING count(*) > 1;
```

In each group, keep one user, then delete the others or set their
`provider_uid` to `NULL`, and run `migrate` again.

**Migration 0010 signs everyone out of the UI once.** It adds
`rotten.users.session_generation` (`bigint not null default 0`), which the UI
bumps to end all of a user's sessions; it's quick and doesn't rewrite the
table. UI sessions started before the matching UI release have no generation
or expiry, so they're refused and users sign in again. See
[ui.md](ui.md#sessions).

**Migration 0011 blocks ingest while it runs, and rewrites
`rotten.event_context`.** It adds `logical_source_id` and `attributed_time`
(the context's share of its event's time, `events.time * c / sum(c)` over the
event's contexts) to `rotten.event_context`. The server writes both at
ingest, so the replica utilization reports read `event_context` alone. The
migration backfills every existing row with one `UPDATE`, then builds
`event_context_source_window` on `(logical_source_id, observed_window_start)`
with a plain `CREATE INDEX`. In testing, that took about 7 minutes for 13
million context rows. The `UPDATE` leaves the old row versions behind, so the
existing partitions stay about twice their size until retention drops them.

Stop `rotten-server serve` before you run this `migrate`, and start the new
release afterwards. A `serve` from before 0011 inserts context rows without
the new columns, and the reports skip those rows until they're filled in.
That includes inserts that waited out the backfill and anything ingested
until the old `serve` stops. Workers keep their harvests in their outbox
while `serve` is down and resend them afterwards. If old rows get in anyway,
for example during a rolling deploy, the new `serve` fills them in with
`rotten.repair_context_utilization()` when it starts and then every hour, a
batch of 1,000 events at a time. It logs `repaired context utilization` with
the row count each time. A partial index, `event_context_utilization_missing`,
finds those rows, and it's empty the rest of the time.

## 4. Partition maintenance and retention

`rotten.events` and `rotten.event_context` are partitioned by day on
`observed_window_start`. pg_partman does two things for them, both in its
maintenance run:

- It creates partitions ahead of time. If maintenance never runs, the
  pre-made partitions run out after a few days, and new rows go to the
  `events_default` and `event_context_default` partitions, where retention
  can't drop them.
- It drops partitions older than the retention period. There's no other
  cleanup; rotten never deletes events.

### Retention

The default retention is 21 days. Set it with `-retention` or
`ROTTEN_RETENTION` on `migrate`. The flag wins over the environment
variable:

```sh
rotten-server migrate -retention 45d
ROTTEN_RETENTION='45 days' rotten-server migrate
```

The value is a whole number of days from 1 through 3650, written as
`45 days`, `45d`, or a Go duration such as `1080h`. An invalid value stops
`migrate` before it changes anything. `migrate` writes the value to
`public.part_config` on every run, so **a run without the setting puts
retention back to 21 days**. Pass the same value every time.

### The background worker, and the permission it needs

Run maintenance with pg_partman's background worker (BGW). Set these in
`postgresql.conf` and restart Postgres:

```ini
shared_preload_libraries = 'pg_partman_bgw'   # add to any existing entries
pg_partman_bgw.dbname = 'rotten'
pg_partman_bgw.role = 'rotten_owner'
pg_partman_bgw.interval = 3600                # seconds between runs
```

**The BGW role must be able to drop `rotten_owner`'s partitions.** In
Postgres, only a table's owner (or a member of the owning role, or a
superuser) can drop it. `migrate` runs as `rotten_owner`, so `rotten_owner`
owns every partition, and pg_partman creates later partitions with the same
owner. pg_partman's default BGW role is `postgres`, which works only if
`postgres` is a superuser. Pick one of these:

- Set `pg_partman_bgw.role = 'rotten_owner'`, as above. This is the simplest
  choice. We checked it: a BGW running as `rotten_owner` drops expired
  partitions.
- Use another role that's a member of `rotten_owner`
  (`GRANT rotten_owner TO partman_role;`) and has pg_partman's privileges.
- Use a superuser.

If the role can't drop a partition, maintenance fails with a permission error
in the Postgres log, and old partitions pile up.

If you can't preload the BGW (some managed services don't allow it), schedule
pg_partman's maintenance procedure as `rotten_owner` instead, for example
hourly from cron or pg_cron:

```sql
CALL public.run_maintenance_proc();
```

**Check that retention works.** Don't rely on the BGW's startup message;
it's logged before the BGW connects, so it shows up even when the role is
wrong. Instead:

1. After the first interval, look in the Postgres log for errors from
   pg_partman's maintenance run, such as `must be owner of table`. A
   successful run may log nothing.
2. List the partitions and confirm that, once retention plus an interval has
   passed, none is older than the retention period, and the newest ones are a
   few days ahead of today:

   ```sql
   SELECT partition_schemaname, partition_tablename
     FROM public.show_partitions('rotten.events');
   ```

   The partition names end in their date. Run the same check for
   `rotten.event_context`. If old partitions stay, the BGW isn't running or
   can't drop them.

## Roles and what each may do

`migrations/permissions.sql` is the source of truth. The application roles
get nothing outside the `rotten` schema, and `PUBLIC` gets nothing in it.

| Role | Used by | May |
| --- | --- | --- |
| `rotten_owner` | `rotten-server migrate`, `rotten-server keys`, pg_partman maintenance | Owns the database, the `rotten` schema and every table and partition. Runs migrations and grants. Creates, lists and revokes pass keys. Needs pg_partman's privileges in `public`. Use it only for administration, never for a long-running service. |
| `rotten_ingest` | `rotten-server serve` | Read every event and lookup table. Insert into `controllers`, `actions`, `job_tags`, `logical_sources`, `physical_sources`, `logical_physical_sources`, `fingerprints`, `events`, `event_context` and `fingerprint_stats`, and update `fingerprint_stats`, `logical_sources.project` and `physical_sources.fqdn`. Never delete. On `api_keys`, read only `id`, `name`, `secret_hash`, `fqdn` and `revoked_at`, and update only `last_used_at`. Read and insert `ingested_batches`, and prune it only through `rotten.prune_ingested_batches()`. Fill in `event_context` rows that a pre-0011 server wrote only through `rotten.repair_context_utilization()`. |
| `rotten_ui` | The Rails UI | Read every event and lookup table. On `api_keys`, read everything but `secret_hash`, insert only `name`, `secret_hash`, `fqdn` and `created_by`, and update only `revoked_at` and `revoked_by`; it can't delete a key. Select, insert, update and delete `users`, including bumping `session_generation`. Read `ui_audit_log`, and insert into it without setting `id` or `at`; it can't change or delete rows. |
| `rotten_readonly` | People running `reports/*.sql` in psql, dashboards | Read the event and lookup tables only. No access to `api_keys`, `ingested_batches`, `users` or `ui_audit_log`. |

The observed databases have their own role, `rotten_observer`. See
[observed.md](observed.md).

Functions that `rotten_owner` creates don't get the default `EXECUTE` for
`PUBLIC`; each one is granted explicitly.

## Running reports by hand

Each report is a SQL file in `reports/`. Connect as `rotten_readonly` and
bind the parameters listed at the top of each file. The UI runs the same
files as `rotten_ui`.
