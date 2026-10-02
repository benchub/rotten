# Rotten modernization plan.

This doc holds the "why." `BACKLOG.md` holds the "what next." Finished tasks move to `BACKLOG-COMPLETE.md`.

Written October 1, 2026. Updated the same day with answers to the open questions (see "Decisions").

## What we found in the current code.

These came from reading the code and probing real Postgres containers on October 1, 2026.

- **The worker can't read Postgres 17 or 18 today.** Postgres 17 renamed `blk_read_time` and `blk_write_time` to `shared_blk_read_time` and `shared_blk_write_time`. The old `schema/functions-pg13.sql` installed on 17 and 18, but every call failed with `column ex.blk_read_time does not exist`. (Fixed by `internal/pgss.Reader`; the dba read functions are gone.) So item 4 isn't only about 18. It's about 17, too.
- **It doesn't build on current macOS.** `pg_query_go` v5 fails to compile against the current macOS SDK (`static declaration of 'strchrnul'`). v6.2.5 (released September 30, 2026) builds fine. It also carries a security fix for `Normalize`.
- **No Go release of `pg_query` has the Postgres 18 parser yet.** `pg_query_go` v6 uses the Postgres 17 parser. `libpg_query` has an `18-latest` branch, but there's no `pg_query_go/v7` release. Queries that use 18-only grammar fail to parse. They get counted and skipped rather than crashing the worker. The corpus now pins these results under v6.2.5; an integration test executes all six cases on real Postgres 18 so malformed fixtures can't masquerade as parser limitations:

  | Corpus case | Postgres 17 parser result |
  | --- | --- |
  | `pg18_returning_old_new` (`RETURNING OLD.name, NEW.name`) | Parses as qualified column references; the parser doesn't validate the new OLD/NEW semantics |
  | `pg18_returning_with_aliases` (`RETURNING WITH (OLD AS o, NEW AS n)`) | `failed to parse` |
  | `pg18_generated_virtual` (explicit `VIRTUAL`) | `failed to parse` |
  | `pg18_generated_default_virtual` (omitted storage kind) | `failed to parse` |
  | `pg18_without_overlaps_primary` | `failed to parse` |
  | `pg18_without_overlaps_unique` | `failed to parse` |

  The worker's existing `fingerprints failed` counter counts failed fingerprint attempts among the top 100 selected entries, not calls or every pg_stat_statements row. Count and samples reset at the start of each harvest. It keeps the first five failed query texts, capped at 1,024 bytes each plus `...` when truncated. The window-end summary logs the count and quoted samples even if the next harvest runs before the progress reporter. Progress reports also show the current harvest's count and samples. These bounded summaries supplement the existing individual parser error logs; they don't change the worker config. Samples are query text and may contain sensitive literals, so treat the worker logs accordingly.
- **Postgres 18 squashes IN lists.** `IN (1,2,3,4)` shows up in pg_stat_statements as `IN ($1 /*, ... */)`. `pg_query_go` v6 gives that the same fingerprint as `IN ($1)` and `IN ($1,$2,$3)`, so history should line up across versions. A test will lock that in.
- **The example queries don't match the schema.** They filter on `observed_window`, but `events` only has `observed_window_start` and `observed_window_end`. The "most expensive" query also has `LIMIT 20` with no `ORDER BY`, so it returns an arbitrary 20. The replica-utilization queries use inner joins, so a job that only runs on one side disappears from the results.
- **`schema/tables.sql` used the pg_partman 4 API.** pg_partman 5 dropped the `'native'` argument to `create_parent`. Task -2 fixed it, and the schema now lives in `migrations/0001_baseline.sql`.
- **The Postgres 13 helper function adds plan and exec stats together.** `min_plan_time + min_exec_time` and `stddev_plan_time + stddev_exec_time` aren't valid stats. Summing totals is fine. We'll use exec-only values for min, max, and stddev.
- **Smaller bugs that tests will likely surface:**
  - Shared counters have data races.
  - `processEvent` leaks its "still processing" counter when it returns early.
  - The config decode error and regex compile errors get ignored.
  - `fingerprint_stats.last` is an `integer` holding Unix seconds, so it overflows in January 2038.
  - `noIdleHands` works by writing to a nil map on purpose.

## Target architecture.

```
observed Postgres (primary or replica)
   ^  read only: pg_read_all_stats (plus a min/max-only reset wrapper on 17+)
   |
rotten-worker (Go)
   - reads pg_stat_statements, diffs against a local snapshot
   - fingerprints queries and pulls out controller, action, and job context
   - local state store (SQLite): snapshot table + outbox of unsent batches
   |
   |  HTTPS (TLS 1.3), "Authorization: Bearer <pass key>", protobuf over Connect RPC
   v
rotten-server (Go, write only; no cgo)
   - checks pass keys on every call (so revocation takes effect right away)
   - validates input, resolves IDs, writes in one transaction, ignores duplicate batches
   |
   v
rotten DB (Postgres 18 + pg_partman)
   ^
   |  read-only role (the existing "rotten-interface")
rotten-ui (Rails; Okta or password login; canned reports; pass key admin)
```

The worker can reach the server, but not the rotten DB. The server and UI share the rotten DB as their only touchpoint. The UI never talks to workers or the server directly.

### Why split write-only ingest from the read UI.

The ingest server faces untrusted networks. The UI sits behind Okta. Splitting them means each has its own small set of database grants: insert and select for ingest, and select for the UI. A bug in one can't widen the other's access. They also deploy, scale, and fail on their own. The database schema is the contract between them.

### Why Go is still the right choice for the worker and server.

- Workers run on or near database hosts. A single binary with no runtime is easy to ship there.
- `pgx` is one of the best Postgres drivers in any language.
- `pg_query_go` is a first-party binding from the pganalyze team, and fingerprinting depends on it.
- The server and worker share the protobuf types, test harness, and fingerprint code.

Rust would also work, but it means a rewrite that doesn't buy us anything we need. Recommendation: stay on Go and move to the latest stable release (1.27.1 today).

### Where fingerprinting happens.

On the worker. That keeps the `pg_query` C parser away from input that comes in over the network. The server stays pure Go, so it builds as a static binary. It also spreads parsing CPU across workers and keeps batches small. The server trusts the fingerprint a worker sends. Tying each pass key to one source limits how much damage a stolen key can do.

### Transport: Connect RPC over HTTPS.

[Connect](https://connectrpc.com) handlers serve the gRPC, gRPC-Web, and plain HTTP/1.1 + protobuf protocols from one `net/http` server. That means:

- It works through any load balancer or proxy, even ones that mangle HTTP/2.
- It's easy to test with `httptest`.
- It still gives us a typed protobuf contract with breaking-change checks (`buf breaking`).

We'll use **unary calls over a long-lived, kept-alive connection**, not long streams. A batch goes out every observation window, so streaming adds complexity without much benefit. Unary calls also make revocation simple: the server checks the key on every call.

### Fault tolerance.

- **Exactly-once effect.** Each batch has a deterministic `batch_id` built from the source and the window. The server records it in `ingested_batches` in the same transaction as the data. A retried batch gets acked without writing anything twice.
- **Source binding.** `physical_sources` stays unique by `fqdn`, so one host has one physical row even if it serves more than one logical source. `logical_physical_sources` links each logical source to the physical hosts that may report for it; SubmitHarvest accepts only linked pairs whose physical fqdn matches the pass key.
- **Durable outbox.** The worker writes the batch and the new snapshot to its local store in one transaction, then sends it. It deletes the batch only after the server acks. If the server is down, batches pile up (with a size limit) and replay in order later. A crash at any point either leaves the window unsent, so it gets retried, or already sent, so it's deduplicated. It never gets counted twice.
- **No `log.Fatal` on network errors.** Both connections retry with exponential backoff and jitter. A failed sanity check still exits on purpose (see the README's reasoning).

### Pass keys.

- Format: `rotten_<key id>_<random secret>` with 32 or more bytes of entropy.
- The server stores only `sha256(secret)`, which is safe for high-entropy random secrets, and compares in constant time.
- The `api_keys` table has `name`, `created_at`, `last_used_at`, `revoked_at`, and an optional pinned `fqdn`.
- Revocation takes effect within a configurable cache TTL (default 30 seconds).
- Admins manage keys through a `rotten-server keys` CLI first, which uses the `rotten_owner` connection, then through UI pages later.

### pg_stat_statements diffing (item 2).

- **Read directly.** Grant `pg_read_all_stats` to the observer role instead of using SECURITY DEFINER functions in a `dba` schema. That removes two TODOs: the hard-coded function location and the hard-coded role name. The observer gets `pg_read_all_stats`, plus on 17+ one SECURITY DEFINER wrapper that only does the min/max reset. `schema/observer.sql` sets this up, with the role and schema names as psql variables (defaults `rotten_observer` and `rotten`).
- **Fetch cheaply.** Each harvest calls `pg_stat_statements(showtext := false)` to get every row. Diffing needs every row, and the slow part of the old full fetch was the query text. Text is fetched only for keys we haven't cached yet.
- **Supported versions:** Postgres 14 through 18.
- **Key:** `(userid, dbid, toplevel, queryid)`.
- **Delta rules**, applied to each entry and checked in this order:
  1. Global reset: `pg_stat_statements_info.stats_reset` changed. Treat every entry as new.
  2. Entry reset: `stats_since` changed (17+). Treat the entry as new.
  3. Your rule: any cumulative counter is lower than the snapshot. Treat the entry as new.
  4. Not in the snapshot: treat it as new.
  5. Otherwise, the value for this window is current minus snapshot.

  "Treat as new" means we use the current values as-is and replace the snapshot entry. Entries that disappear (evicted) get dropped from the snapshot.
  - **Limitation on 14 through 16:** there's no `stats_since`, so rule 2 can't fire. If an entry is reset on its own and grows past its old counters before the next harvest, we can't detect it, and that window's delta comes out too small.
  - Rule 3 also treats an optional counter that's present on only one side as new.
  - Entries with zero delta calls aren't sent, but they stay in the snapshot. That also drops planning-only activity.
- **First run with no snapshot:** record a baseline and send nothing, the way the old worker reset and then slept. A snapshot older than `MaxSnapshotAge` (default three windows) also counts as a baseline, so we never send one huge window. So does a state store error at runtime, or a failed save on the harvest before (the stored snapshot is then behind what was sent, and diffing against it would count that window twice). The worker logs these and keeps going. The worker never runs a full reset.
- **Mean and stddev:** mean is Δtotal_exec_time / Δcalls (exec only, matching pg_stat_statements' exec-only mean and stddev). For stddev, we rebuild each side's sum of squares from `stddev² × calls`, then subtract with the parallel-variance formula. When the subtraction cancels badly (a small window after a huge history), the stddev is flagged as unreliable instead of reported: the worker leaves it out of that window's `fingerprint_stats` sample rather than recording a 0.
- **Min and max can't be diffed.** On 17+, the worker calls `<schema>.pg_stat_statements_minmax_reset()` after each harvest, a wrapper that runs `pg_stat_statements_reset(0, 0, 0, minmax_only := true)`. The wrapper resets only min and max, not the counters. We can't grant the raw function: `minmax_only` defaults to false, so EXECUTE on it would also allow a full reset. On 14 through 16, we report the entry's lifetime min and max and flag them as lifetime values.
- **Top N:** pick the top 100 for each metric by delta, in Go. This replaces the 19-way SQL `UNION`.
- **Where the snapshot lives:** a table in the worker's local SQLite store, the same file that holds the outbox. Two reasons:
  - Replicas are read-only, so the snapshot can't live on the observed database.
  - Updating the snapshot and enqueuing the batch together is what prevents double counting.

### Testing approach (item 6).

- **Test-driven from here on.** Every task starts with a failing test. Infrastructure tasks start with a failing smoke check.
- **Real databases in Docker.** We'll use [testcontainers-go](https://golang.testcontainers.org):
  - Observed databases: Postgres 14 through 18, with `pg_stat_statements` preloaded and `track_planning` on.
  - Rotten database: Postgres 18 with pg_partman.
- **Characterization tests first.** Before changing behavior, we pin what the code does today, including a golden file of fingerprints from `pg_query_go` v5. Then we upgrade and refactor under those tests. We're not keeping old data, so a fingerprint change from the upgrade gets noted, not migrated.
- **Make targets:** `make test` (Go, in Docker, race detector on), `make test-unit` (`-short`, in Docker without the Docker socket, until the pg_query_go upgrade in -6 restores native builds), `make test-ui` (RSpec, in Docker), and `make test-all`. There's no CI. These targets are the gate, and every task runs them before it's done.

### Network layout in tests and dev.

`dev/docker-compose.yaml` and the end-to-end tests use three Docker networks so that a wrong connection fails instead of quietly working:

| Network | Members | Why |
|---|---|---|
| `observed` | observed Postgres, worker | The worker reads stats. |
| `edge` | worker, server | This stands in for the untrusted network. TLS and pass keys are required. |
| `core` | server, UI, rotten DB | This is the shared database touchpoint. |

The worker isn't on `core`. An end-to-end test checks that the worker can't reach the rotten DB.

### UI (item 5).

**Decided: Rails.** That means:

- Rails 8.1 on Ruby 3.4.
- RSpec, Capybara, FactoryBot, and Shoulda Matchers.
- importmap, Turbo, Stimulus, and standalone Tailwind (no Node).
- A dev image with Chromium for system specs, kept separate from the production image.
- A `spec/security/` suite covering CSRF, headers, session fixation, and provisioning fuzz tests.

The report SQL lives in `reports/*.sql`, tested against a seeded rotten DB from the Go harness. The UI runs those files, so reports get tested before the UI exists.

**Auth is generic, with the mode picked by config.** `ROTTEN_UI_AUTH=oidc|password`.

- **`oidc`** uses `omniauth_openid_connect`. Everything specific to an org comes from env, with no defaults baked in: `OIDC_ISSUER`, `OIDC_CLIENT_ID`, `OIDC_CLIENT_SECRET`, `OIDC_GROUPS_CLAIM`, `ROTTEN_UI_VIEWER_GROUP`, and `ROTTEN_UI_ADMIN_GROUP`.
  - Okta is just one issuer. Okta settings live in the deploy repo, not here.
  - We provision users at login: match on `sub`, then on email, then create a new user.
  - A local `active` flag acts as a kill switch.
  - Group membership resyncs on every login, and it fails closed. If `ROTTEN_UI_VIEWER_GROUP` isn't set, any authenticated user is a viewer.
  - For local dev, `OMNIAUTH_FAKE=1` logs in offline, and it only works in development.
- **`password`** uses `has_secure_password` (bcrypt) on the same `users` table, with login rate limiting. There's no self sign-up. Admins manage users with `bin/rails users:create|disable|reset_password`.
- Both modes give every user one of two roles: viewer or admin.

### Schema migrations and database roles.

We're not keeping existing data, so we start fresh.

- **Migrations.** We'll use [goose](https://github.com/pressly/goose) SQL migrations in `migrations/`, embedded in the server binary. `rotten-server migrate` applies them. Migration 0001 is a cleaned-up version of today's `tables.sql`: pg_partman 5 API, `fingerprint_stats.last` as `bigint`, and no legacy roles.
- **One migration owner.** goose owns every table, including the UI's (`users` and `ui_audit_log`). Rails uses `schema_format :sql` and never runs migrations. Its test database gets prepared by `rotten-server migrate`.
- **Grants, table by table.** `migrations/permissions.sql` revokes everything, then grants exactly what each role needs, table by table. It gets reapplied after every migrate. There's no blanket grant, so a new table is unreachable until someone lists it.
- **The roles:**

| Role | Used by | Can do |
|---|---|---|
| `rotten_owner` | `migrate` only | Owns the schema and all DDL. |
| `rotten_ingest` | Server | Inserts into and selects from the event tables and `logical_physical_sources`, updates `fingerprint_stats`, updates only `project` on `logical_sources` and `fqdn` on `physical_sources` for atomic registration upserts, selects the auth columns of `api_keys` and updates only `last_used_at`, inserts into and selects from `ingested_batches` with source/window/content metadata, and runs `prune_ingested_batches()`. Can't create or revoke keys, and can't delete anything. |
| `rotten_ui` | UI | Selects from the event tables. Has DML on `users`. Inserts into and selects from `api_keys` (never `secret_hash`) and `ui_audit_log`, and updates only `revoked_at` and `revoked_by` on `api_keys`. |
| `rotten_readonly` | People at a SQL prompt | Selects from the event tables. |

### Build artifacts.

The servers run on some host as long-running processes. A separate deploy repo consumes this one, so this repo provides:

- `make build`, which produces the `rotten-worker` and `rotten-server` binaries.
- Production Dockerfiles: `docker/server.Dockerfile`, `docker/worker.Dockerfile`, and `ui/Dockerfile`.
- An env and config reference.

It doesn't include platform manifests.

### Repo layout we're aiming for.

```
cmd/rotten-worker/   cmd/rotten-server/
internal/fingerprint internal/pgss (reader + diff) internal/state (SQLite)
internal/ingest (server write path) internal/auth internal/testdb
proto/rotten/v1/     gen/ (buf output)
migrations/          reports/          ui/        docker/       dev/ (compose stack)
```

## Decisions.

These answer the open questions from the first draft (October 1, 2026).

1. **Snapshot:** a table in the worker's local SQLite store.
2. **Existing data:** We don't keep it. We start with a fresh schema and accept any fingerprint changes from v5 to v6.
3. **Postgres versions:** 14 through 18. There are no Postgres 13 code paths.
4. **UI:** Rails, with generic `oidc` or `password` auth. Okta specifics stay in the deploy repo.
5. **Observer grants:** `pg_read_all_stats`, plus on 17+ one SECURITY DEFINER wrapper that only does the min/max reset. This replaces the old SECURITY DEFINER functions.
6. **Deployment:** Each piece runs on a host as a long-running process, and a separate repo deploys it. There's a network between workers and the server. The server and UI share only the rotten DB.
7. **CI:** none for this repo. The `make test*` targets are the gate.

## More ideas, not in the backlog yet.

- Store more metrics on each event (rows, block reads, I/O time, WAL bytes). Today `events` only keeps `calls` and `time`, so the UI can't chart the rest. We're starting from a fresh schema, so this is the cheapest it'll ever be.
- Handle primary and replica role changes. The worker could detect `pg_is_in_recovery()` and report its role itself. That's more reliable than a static `Role` config.
- Address the `fingerprint_stats` hot rows. Every worker updates the shared `logical_source_id = 0` rows, which means 19 rows per fingerprint. If lock waits show up, fold the 19 types into one row per fingerprint and source.
- Expose Prometheus metrics from the server and worker (batches queued, lag, parse failures).
- Upgrade to `pg_query_go` v7 when it ships the Postgres 18 parser and the user confirms the release is out; regenerate and review the fingerprint golden file, including the `pg18_*` cases above.
