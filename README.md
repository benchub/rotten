rotten
======
Requests Over Time, Tracked Easily Now is a project written to help optimize your app
by letting you know which queries it is murdering your DB with. A reasonable person
might ask, "Isn't this why we have pgBadger?" to which I would say that while
pgBadger provides excellent analytics in a readable format,

1. it requires full logging to not be misleading
2. full logging is problematic when you're logging to a remote server for compliance
   reasons, and also already approaching the packets/sec limits of your hardware 
   _before_ enabling full logging.
3. pgBadger reports are generated once for a static set of queries. If you want to
   drill in on a subset of data, that doesn't work.

rotten attemps to address these things. It does so by frequently looking at the helpful
data gathered by pg_stat_statements and recording how much each counter grew since the
last look. This has shortcomings, but it gets us 95% of the
usefulness of logging all queries in order to see which are problematic, without any of
the firehose problems that full logging can bring.

Instead of recording our analysis to text files, like pgBadger does, we just store the
data in an additional database and punt on any form of UI. Yes, that's a bit of a cop
out, but it also lets us employ the power of SQL to filter our data and pull whatever 
report we might want, for whatever time period we might want.

Assumptions
===========
As a young project rotten makes a lot of assumptions. Among them:

1. You are using pg_stat_statements. Rotten doesn't reset its counters, so other tools that
   read them keep working. On Postgres 17 and later, it does reset min and max after each
   harvest.
2. You are ok with not getting everything from pg_stat_statements, but rather the "most"
   interesting queries. Getting *everything* is orders of magnitude too expensive to be
   useful on busy systems, and anyway, this project is only trying to find the worst 
   offenders, not an exhaustive snapshot.
3. You are ok with a SQL prompt as a UI for now.
4. You will have an observer role on your monitored dbs (default `rotten_observer`) that
   reads pg_stat_statements directly through `pg_read_all_stats`. On Postgres 17 and later,
   it can also call one security definer function that resets only min and max.
5. Your monitored databases have distinct identifiers of some kind (fqdn, IP, etc) as well
   as some logical identification ("the primary production server" or "cluster38 secondary").

How to use it
=============
1. Get and install Go 1.27 or later. http://www.golang.org
2. Build it. A native build needs cgo and a C compiler, because pg_query_go compiles
   libpg_query from C:
  ```bash
  go build ./cmd/rotten-worker
  ```
   That writes a `rotten-worker` binary in the current directory.
   Native builds work on macOS and Linux. `make test-unit` runs natively, and `make test`
   runs the full suite in Docker.
3. Install pg_partman in the rotten db. See https://github.com/pgpartman/pg_partman. tldr:
 - download pg_partman and `make install`
 - add `pg_partman_bgw` to `shared_preload_libraries` in postgresql.conf
 - run `CREATE EXTENSION pg_partman` in the rotten db
4. Create a `rotten_owner` login role, make it the owner of the rotten db, and grant it
   pg_partman's non-superuser privileges (all on pg_partman's tables and sequences, and
   execute on its functions and procedures). Then build and run the migrations as that role:
  ```bash
  go build ./cmd/rotten-server
  ROTTEN_OWNER_DSN='postgres://rotten_owner@host/rotten' ./rotten-server migrate
  ```
   Running `migrate` again is safe; it applies only what's new. Grants for the other
   roles are reapplied on every run from `migrations/permissions.sql`.

   **Partition retention.** pg_partman drops `events` and `event_context` partitions
   older than the retention period. The default is 21 days. To change it, pass
   `-retention` or set `ROTTEN_RETENTION` (the flag wins):
  ```bash
  ./rotten-server migrate -retention 45d
  ROTTEN_RETENTION='45 days' ./rotten-server migrate
  ```
   It takes a whole number of days, from 1 through 3650, written as `45 days`, `45d`, or a
   Go duration like `1080h`. `migrate` writes it to `public.part_config` on every run, so
   a run without the setting puts retention back to 21 days. Pass the same value every
   time. An invalid value fails `migrate` before it changes anything.

   **Server keys.** Workers authenticate to `rotten-server` with a pass key in an
   `Authorization: Bearer` header. Manage keys as `rotten_owner` through `ROTTEN_ADMIN_DSN`:
  ```bash
  export ROTTEN_ADMIN_DSN='postgres://rotten_owner@host/rotten'
  ./rotten-server keys create --fqdn db1.example.com db1-worker
  ./rotten-server keys list
  ./rotten-server keys revoke db1-worker
  ```
   `create` prints the key once. The database keeps only a SHA-256 hash of its secret, so
   a lost key can't be recovered; revoke it and create a new one. `list` never shows
   secrets. `--fqdn` pins a key to one worker host. The server refuses to register or
   accept harvests for any source with an unpinned key, so give every worker key `--fqdn`.
   A revoked key stops working within the server's key cache TTL, 30 seconds by default.

   **HTTPS ingest server.** Run the listener as `rotten_ingest`, separately from
   migrations and key administration:
  ```bash
  ROTTEN_INGEST_DSN='postgres://rotten_ingest@host/rotten' \
    ./rotten-server serve -listen :8443 \
    -tls-cert /path/to/server-chain.pem -tls-key /path/to/server-key.pem
  ```
   TLS 1.3 is the minimum; there is no plaintext listener or TLS 1.2 fallback.
   Clients must trust the server's CA and verify its hostname. Worker bearer keys,
   not client certificates, authenticate RPCs. The existing Connect API is mounted
   with pass-key authentication, including HTTP/2 support. `Register` creates
   or reuses source rows for the worker. `SubmitHarvest` writes one harvest
   batch transactionally and deduplicates retries by `batch_id`.

   The server can also read a JSON config file with the same exported-key
   format as the worker config:
  ```json
  {
    "DSN": "postgres://rotten_ingest@host/rotten",
    "Listen": ":8443",
    "TLSCert": "/path/to/server-chain.pem",
    "TLSKey": "/path/to/server-key.pem",
    "ShutdownTimeout": 10,
    "HealthTimeout": 1
  }
  ```
   `ShutdownTimeout` and `HealthTimeout` are whole seconds. Precedence is
   flags, then `ROTTEN_SERVER_*` environment variables, then the config file,
   then defaults. For compatibility when no config file is used, the old
   `ROTTEN_INGEST_DSN`, `ROTTEN_LISTEN`, `ROTTEN_TLS_CERT`, and
   `ROTTEN_TLS_KEY` names still work.

   | Flag | Config key | Environment override | Default |
   | --- | --- | --- | --- |
   | `-config` | n/a | n/a | No config file |
   | `-dsn` | `DSN` | `ROTTEN_SERVER_DSN` | Required; use `rotten_ingest` |
   | `-listen` | `Listen` | `ROTTEN_SERVER_LISTEN` | `:8443` |
   | `-tls-cert` | `TLSCert` | `ROTTEN_SERVER_TLS_CERT` | Required PEM certificate chain, leaf first |
   | `-tls-key` | `TLSKey` | `ROTTEN_SERVER_TLS_KEY` | Required matching PEM private key |
   | `-shutdown-timeout` | `ShutdownTimeout` | `ROTTEN_SERVER_SHUTDOWN_TIMEOUT` | `10` seconds |
   | `-health-timeout` | `HealthTimeout` | `ROTTEN_SERVER_HEALTH_TIMEOUT` | `1` second |

   Explicit flags override env and config values. The certificate and key must load before
   the server connects to the database or opens its listener; invalid files abort
   startup. Keep the key file readable only by the server's service account.
   Replace both files to rotate certificates. The server reads their contents every
   second (including files replaced by rename or symlink swaps, even with unchanged
   timestamps), and SIGHUP forces an immediate reload. Only a successfully parsed,
   matching pair replaces the active certificate. Missing, malformed, or mismatched
   files log reload errors and leave the last good pair active; polling retries
   until the files are repaired. This does not validate certificate expiry or CA
   trust on the server; clients still enforce those checks.
   New TLS connections use the new certificate; existing connections remain open
   with their original TLS session. TLS session resumption is disabled to ensure
   reconnecting clients always verify the current certificate. SIGINT/SIGTERM stop
   accepting new connections, drain in-flight requests for up to
   `ShutdownTimeout`, and stop the certificate reload and prune loops. If that
   timeout expires, the server cancels outstanding request contexts, force-closes
   HTTP connections so database transactions roll back, logs the timeout, and
   exits nonzero instead of waiting indefinitely for pooled connections. The
   unauthenticated `GET /healthz` readiness endpoint does a short database ping:
   it returns `200 ok` when the rotten DB is reachable and `503 unhealthy` when
   it is not, without including database error text in the response. Use it for
   load-balancer or Kubernetes readiness; process liveness is still the service
   manager's job.

   **Ingest validation limits.** The server rejects semantically invalid
   `SubmitHarvest` and `Register` requests before opening a database transaction.

   | Input | Limit |
   | --- | ---: |
   | Connect request body | 32 MiB |
   | Fingerprint aggregates per harvest | 2000 |
   | Query-context entries per harvest | 2000 |
   | Fingerprint string | 128 bytes |
   | Normalized query string | 8 KiB |
   | Context strings | 512 bytes |
   | Floating metric values | 1e15 ms |
   | Harvest window duration | 24 hours |
   | Harvest window future skew | 5 minutes |
   | Register source strings | 255 bytes |
   | Register `worker_version` | 128 bytes |

   The aggregate and context caps cover the worker's current top-N selection:
   the union of 100 entries for each of 19 metrics, rounded up to 2000. The
   24-hour window limit leaves room for valid worker configurations and outbox
   replay; there is no "too far in the past" check. Future worker RPC sending
   must truncate normalized query strings to 8 KiB at a UTF-8 boundary before
   sending. Window times must be ordered and no more than five minutes ahead of
   the server clock. Numeric metric fields must be finite and non-negative. All
   stored text must be valid UTF-8 and cannot contain NUL bytes, matching
   PostgreSQL `text`.
5. Install `pg_stat_statements` in the monitored database:
 - add `pg_stat_statements` to `shared_preload_libraries` in postgresql.conf (this is a comma-separated string)
 - run `CREATE EXTENSION pg_stat_statements` in the monitored database
6. As a superuser on each monitored database (Postgres 14 through 18), run `schema/observer.sql`
   with psql. The worker never resets pg_stat_statements' counters. It saves a snapshot after
   each harvest and reports the difference. On Postgres 17 and later, it resets only min and
   max after each harvest, through the wrapper that script creates.
   Note: min, max, mean, and stddev times are now exec time only (planning time isn't
   mixed in), while total time is still plan + exec.
7. Unless you like to be webscale with tmux, script up some systemd services to run rotten.
8. Modify the conf to fit your environment.
  1. `RottenDBConn` and `ObservedDBConn` are hopefully self-explanatory. Extra care has been
     given in rotten to make sure that rotten will correct send a root ca with all the needed
     intermediate certs, if you are working with such an environment.
     Until the worker switches to the ingest server, `RottenDBConn` still talks directly to the
     rotten database. Its role must be able to run the source-registration upserts: `SELECT`,
     `INSERT`, sequence `USAGE`, and column `UPDATE` on `logical_sources.project` and
     `physical_sources.fqdn`, matching the `rotten_ingest` grants in `migrations/permissions.sql`.
  2. `SanityCheck` is a query that will be run against the Observed DB before each window.
     Returning a boolean True value will tell rotten to proceed; a False will cause rotten
     to quit. The assumption is that systemd will keep restarting rotten until SanityCheck
     returns True, and also that you have a function you might call which tells you what the
     database you have connected to thinks it is.
     This is useful in environments where the host rotten is connecting to might not be what
     rotten intends. For example, you might want to be gathering statistics from a secondary
     database, but the secondary hostname might currently point to the primary server, while
     the secondary undergoes maintenance. While that might be exactly what most database
     clients would want, it's not helpful for rotten's purposes. Potentially worse, rotten
     would not know when to reconnect once maintenance is done, and so would stay connected to
     the primary until it dies or is manually restarted.
  3. `StatusInterval` is how often to report status (in seconds) to its log.
  4. `ObservationInterval` is how long (in seconds) to let pg_stat_statements gather info
     for. This is the most granular you can make your reports, and the lower you set this,
     the more data you will need to store in your rotten db.
  5. `FQDN` is some unique string (typically the FQDN of the observed db) to help find a
     physical log if more information is desired other than the fingerprint.
  6. `Project`, `Environment`, `Cluster`, and `Role` are logical identifiers for where the samples
     of data are coming from.
  7. `KeepSchemas` is optional and defaults to `false`. By default, rotten ignores schema
     names when it fingerprints queries, so `users`, `public.users`, and `shard_1.users`
     all group together. Set it to `true` if your schemas mean different things and you
     want their queries kept apart.
  8. `CursorPattern` and `TempTablePattern` are optional. They're regexes that match the
     cursor and temp-table names your app or ORM generates, so a fresh random name doesn't
     make a fresh fingerprint. Leave them out to use the defaults shown in `conf`:
     cursors look like `users_cursor_ab12`, and temp tables look like
     `users_temp_table_qwerty`, with a random suffix of six or more characters. Each
     pattern needs exactly two capture groups: the prefix before the generated part and the
     suffix after it. Write any other grouping as `(?:...)`. A pattern that matches the
     empty string is rejected. Rotten keeps those and replaces the middle with `_cursor_x` or
     `_temp_table_x`. Write `CursorPattern` unanchored. Rotten anchors it to whole names
     when it walks the parse tree and uses it as is to search whole statements. To match
     your generator, look at real names in `pg_stat_statements` and fit the random part.
     For example, if temp tables get a three-character suffix like `users_temp_table_abc`,
     use `([^\\s]+)_temp_table_[0-9a-z]{3}[0-9a-z]*([^\\s]*)`. That's the JSON form, with each backslash doubled.
     Changing a pattern changes fingerprints for the names it matches, so
     history from before and after the change won't line up for those queries. An invalid
     regex stops the worker at startup with an error that names the setting.
  9. `MinmaxResetSchema` is optional and defaults to `rotten`. On Postgres 17 and later, it's
     the schema where `schema/observer.sql` created `pg_stat_statements_minmax_reset()`. Set
     it to match the `observer_schema` you passed to that script. Postgres 14 through 16
     ignore it.
  10. `StateDir` is optional and defaults to `/var/lib/rotten-worker`. It's the directory
     where the worker keeps its local state file, the snapshot of pg_stat_statements from
     its last harvest. The worker creates it if it's missing, so it must be writable by the
     worker's user. Each worker needs its own `StateDir`, since a second worker on the same
     directory refuses to start. If the state is lost, the next harvest is a baseline that
     records nothing, and reporting picks up one window later.
  11. `MaxSnapshotAge` is optional, in seconds, and defaults to three times
     `ObservationInterval`. If the saved snapshot is older than this, say after the worker
     was down for a while, the next harvest is a baseline instead of one huge window.

Known Issues
============
- The code is ugly.

TODO
====
Um yeah quite a bit.

- make a UI
- allow for arbitrary logical source descriptions, not just Project/Environment/Cluster/Role
- allow for arbitrary locations of the functions in the observed db
- allow for an arbitrary observer role name other than "rotten-observer"
- allow the observation window to adjust size as needed for processing
- allow for a worker pool of reparse executions to speed things up in wall time
- configurable context, instead of the hardcoded controller/action/job_tag, with their hard-coded regexes
- keep a log of the queries we can't parse
- While we make an effort to normalize cursors and temp tables, those regexs should probably not be hardcoded.
- Collapse IN () and VALUES clauses of constants, so that IN (1,2,3) is the same as IN (1).
