# Running rotten-worker

Run one `rotten-worker` per observed database. Every `ObservationInterval`
seconds it reads `pg_stat_statements`, works out how much each statement's
counters grew since the last harvest, fingerprints the statements, and sends
the result to `rotten-server`. Harvests wait in a local outbox until the
server accepts them, so the worker rides out server outages and restarts.

Before you start, prepare the observed database ([observed.md](observed.md))
and create a pass key pinned to this worker's `FQDN` ([keys.md](keys.md)).

## Command line

```sh
rotten-worker -config /etc/rotten-worker/worker.json
```

| Flag | Meaning |
| --- | --- |
| `-config` | Path to the JSON config file. Required. With no flags at all, the worker prints its flags and exits. |
| `-noIdleHands` | Turns on a watchdog. If the worker stops making progress for several observation windows, it logs why and exits nonzero, so the service manager restarts it. Harvests, baseline harvests and reconnect attempts all count as progress. The watchdog runs every `StatusInterval`. |
| `-debug` | More logging. |
| `-cpuprofile` | Writes a Go CPU profile to this file. |
| `-memprofile` | Writes a Go heap profile to this file when the worker exits. |
| `-version` | Prints the version and exits. |

The worker logs to two streams, and you need both:

- **stderr, plain text** with a timestamp, from Go's `log` package. This is
  most of the output: startup and config errors, the periodic status line
  (`Current window closed ...`), harvest warnings such as a failed snapshot or
  a failed min/max reset, fingerprinting failures, the outbox cap dropping
  its oldest harvests (`worker outbox cap dropped oldest batches`), and a
  corrupt state file being moved aside.
- **stdout, JSON lines**, from `slog`. These are the outbox and registration
  events: the `worker outbox status` counters (`queued`, `sent`,
  `dropped_cap`, `dropped_rejected`, `dropped_stale_source`), harvests the
  server refused or that will be retried, failed `Register` attempts, a change
  of source registration IDs, and the shutdown flush running out of time.

systemd's journal and container runtimes collect both by default. If you
redirect output yourself, redirect both.

## Configuration

The config is a JSON file. `conf` in the repository root is an example with
every key, and `dev/worker.json` is the one the dev stack uses.

### Connections

| Key | Required | Meaning |
| --- | --- | --- |
| `ObservedDBConn` | yes | A list holding one connection string for the observed database, in libpq keyword or URL form. Only the first entry is used. Connect as the observer role. |
| `ServerURL` | yes | The server's base URL, such as `https://rotten-server.example.com:8443`. |
| `PassKeyFile` | yes | A file holding this worker's pass key and nothing else. Read once at startup. |
| `ServerCAFile` | yes | A PEM file with the CA that signed the server's certificate. It's added to the system's trusted CAs. The connection uses TLS 1.3 and checks the host name. |
| `StateDir` | yes | A directory for the worker's SQLite state: the last snapshot and the outbox. The worker creates it if needed, so its user must be able to write there. Give each worker its own; a second worker on the same directory refuses to start. |

The observed database connection is made with pgx, so the usual libpq
environment variables, such as `PGPASSWORD`, and a `.pgpass` file work.
The worker also handles TLS chains with intermediate CAs. To find the root
certificate, it follows libpq's order: `sslrootcert` in the connection string,
then a `sslrootcert` from a service in the service file (`PGSERVICE` and
`PGSERVICEFILE`, or `service=` in the connection string, with
`~/.pg_service.conf` as the default file), then `PGSSLROOTCERT`, then
`~/.postgresql/root.crt`.

### Timing

| Key | Required | Meaning |
| --- | --- | --- |
| `ObservationInterval` | yes | Seconds between harvests. This is the finest granularity reports can show. A shorter interval stores more rows. |
| `StatusInterval` | yes | Seconds between status lines in the log, and between watchdog checks with `-noIdleHands`. |
| `MaxSnapshotAge` | yes | Seconds. If the saved snapshot is older than this, for example after the worker was down, the next harvest is a baseline that records nothing, instead of one huge window. Three times `ObservationInterval` is a good start. |
| `OutboxCap` | no, default `288` | The most harvests the outbox holds while the server is unreachable. An integer from 1 to 2016. See [State and the outbox](#state-and-the-outbox). |

### Identity

| Key | Required | Meaning |
| --- | --- | --- |
| `FQDN` | yes | The observed host's name. It must match the FQDN the pass key is pinned to (case and a trailing dot don't matter). Reports use it to point you at the right host's logs. |
| `Project` | yes | Logical source: the project this database serves. |
| `Environment` | yes | Logical source: the environment, such as `production`. |
| `Cluster` | yes | Logical source: the cluster. |
| `Role` | yes | Logical source: this host's role in the cluster, such as `primary` or `replica`. The replica utilization reports compare roles. |
| `SanityCheck` | yes | A query run on the observed database before each harvest. It must return one row with `true`. If it returns `false`, `NULL` or no rows, the worker exits nonzero. |

`SanityCheck` protects the data when a host name can point at the wrong
server. Say the worker should watch a replica, but the replica's DNS name
points at the primary during maintenance. A query that asks the database what
it is, such as `select pg_is_in_recovery()`, makes the worker exit instead of
recording the primary's numbers as the replica's. Let the service manager
restart it until the check passes. Lost connections don't trigger an exit;
the worker reconnects with backoff.

### Query context

Rotten pulls a controller, an action and a job tag out of comments in the
query text, like the ones the Rails `marginalia` gem and Rails query logs
add. Each key is a Go regular expression. The value is the last capture
group in the first match, and a statement with no match has no value.

| Key | Required | Example |
| --- | --- | --- |
| `ContextController` | yes | `/\\*.*controller(_with_namespace)?:([^,]+).*\\*/` |
| `ContextAction` | yes | `/\\*.*action:([^,]+).*\\*/` |
| `ContextJob` | yes | `/\\*.*job(_tag)?:([^,]+).*\\*/` |

The examples are in JSON form, with each backslash doubled. If your
application doesn't add comments, use a pattern that never matches, such as
`a^`, since the keys can't be blank. An invalid pattern stops the worker at
startup with an error that names the key.

On Postgres 18, `pg_stat_statements` drops a leading comment from the query
text it keeps, while 14 through 17 keep it. Trailing and inline comments
survive on every version, and Postgres has no setting for this. So on 18,
have your application append its comments instead of prepending them:

- `marginalia` gem: `Marginalia::Comment.prepend_comment = false`
- Rails query logs: `config.active_record.query_log_tags_prepend_comment = false`

Both append by default. If the observed server is 18 or later, the worker
watches for context matches on each connection. After at least 3 harvest
windows and 1,000 calls without a single match, it logs this warning:

```
No marginalia contexts found in sampled calls on PostgreSQL 18+. If your application emits leading comments, PostgreSQL 18 removes them from pg_stat_statements; configure it to append them (e.g. prepend_comment = false)
```

Only top-level statements count, and not the ones run by the worker's
observer role, such as its own reads and sanity check. One match on the connection ends the
check. It warns at most once per worker process, and checks again only after
a restart. It doesn't warn if all three patterns can never match, such as
`a^`.

### Fingerprinting

| Key | Required | Meaning |
| --- | --- | --- |
| `KeepSchemas` | no, default `false` | By default, schema names are ignored when fingerprinting, so `users`, `public.users` and `shard_1.users` group together. Set `true` if your schemas mean different things. |
| `CursorPattern` | no | A regular expression for the cursor names your application generates, so a new random name doesn't make a new fingerprint. The default matches names like `users_cursor_ab12`. |
| `TempTablePattern` | no | The same, for temporary table names. The default matches names like `users_temp_table_qwerty`, with a random part of six or more characters. |
| `MinmaxResetSchema` | no, default `rotten` | On Postgres 17 and later, the schema holding `pg_stat_statements_minmax_reset()`. Match the `observer_schema` you gave `schema/observer.sql`. Ignored on 14 through 16. |

`CursorPattern` and `TempTablePattern` each need exactly two capture
groups: the part before the generated name and the part after it. Write any
other group as `(?:...)`. Rotten keeps the two groups and replaces the middle
with `_cursor_x` or `_temp_table_x`. A pattern that matches the empty string
is refused. Write the pattern unanchored; rotten anchors it when it checks
names. To fit yours, look at real names in `pg_stat_statements`. For
example, for temp tables with a three-character suffix such as
`users_temp_table_abc`, use
`([^\\s]+)_temp_table_[0-9a-z]{3}[0-9a-z]*([^\\s]*)`. Changing a pattern
changes the fingerprints of the statements it matches, so their history
before and after the change won't line up.

### Removed keys

`RottenDBConn`, `LogicalID` and `PhysicalID` belong to the old worker, which
wrote to the rotten database directly. If any of them is present, the worker
stops at startup and says so.

## State and the outbox

`StateDir` holds the snapshot from the last harvest and the outbox. If the
state is lost, the next harvest is a baseline and reporting resumes one
window later. If the state file is corrupt, the worker moves it aside, logs
where it went, and starts fresh. The outbox holds at most `OutboxCap`
harvests, 288 by default, which is a day at a 5-minute interval. The time it
covers is `OutboxCap` times `ObservationInterval`. When it's full the oldest
harvest is dropped. If you lower `OutboxCap` below what's queued, the worker
drops the oldest harvests down to the new cap when it starts, logs
`worker outbox cap dropped oldest batches at startup`, and counts them in
`dropped_cap`.

The upper bound of 2016 is a week at a 5-minute interval. A harvest is
usually tens of kilobytes, but the server accepts up to 32 MiB, so a full
outbox of the biggest possible harvests could take tens of gigabytes. Past a
week, a longer outage is better fixed than buffered. Harvests the server
refuses as invalid are dropped and logged; authentication failures keep them
queued.

## Changing a worker's identity

The worker caches its registration in `StateDir`: `ServerURL`, `Project`,
`Environment`, `Cluster`, `Role`, `FQDN`, and the source IDs the server
assigned. At startup it compares the config with that cache.

- **The identity is unchanged.** The worker starts on the cached IDs and
  re-registers in the background. If the server now answers with different
  IDs, for example because the rotten database was rebuilt, the worker
  re-addresses its queued harvests to the new IDs, logs `source registration
  IDs changed` and exits 1. The service manager restarts it on the new IDs.
- **Any of those six keys changed.** Startup waits until `Register`
  succeeds, retrying with backoff. Queued harvests addressed to source IDs
  other than the ones the server now returns are deleted and counted in
  `dropped_stale_source`. Changing `Project`, `Environment`, `Cluster`,
  `Role` or `FQDN` creates a new source, so the whole outbox goes. Changing
  only `ServerURL` keeps the outbox if the new URL reaches the same rotten
  database, because the IDs come back the same.

So drain the outbox before you change a worker's identity: stop the worker
once the `worker outbox status` line shows `queued` at 0.

## Stopping

`SIGINT` and `SIGTERM` stop the worker cleanly. It finishes the current
harvest, tries for up to 10 seconds to send what's in the outbox, logs what's
still queued (it stays on disk for next time), and exits 0. A second signal,
or a harvest that doesn't finish within those 10 seconds, stops it at once
with exit code 1.

## The container image

`docker/worker.Dockerfile` builds a Debian slim image that runs as user
`65532` with `-config /etc/rotten-worker/worker.json`. Mount the config, the
pass key and the CA file, and a writable volume for `StateDir`, for example
at `/var/lib/rotten-worker`. See [building.md](building.md).

## Running under systemd

A minimal unit:

```ini
[Unit]
Description=rotten-worker for appdb
After=network-online.target

[Service]
User=rotten-worker
ExecStart=/usr/local/bin/rotten-worker -noIdleHands -config /etc/rotten-worker/worker.json
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```
