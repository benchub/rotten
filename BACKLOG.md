# Backlog.

Work top to bottom unless a task says otherwise. Background and reasoning live in `docs/plan.md`.

**Rules.**
- Write a failing test first. Make it pass. Then refactor.
- When a task is done, cut it from this file and paste it at the bottom of `BACKLOG-COMPLETE.md`, with the date and commit SHA.
- New task IDs use the `YYYYMMDD-HHMMSS-N` format: when the task was written, plus a counter.
- Before calling a task done, run `make test-all` (or `make test` before the UI exists). This repo has no CI, so these targets are the gate.

---

## Phase A: Test harness and characterization.

### 20261001-112142-5: Make partition retention a setting.
- **Do:** Keep 21 days as the default for both `events` and `event_context`, but make the retention period configurable when the rotten DB is set up, through `rotten-server migrate` or a server setting.
- **Red test:** A non-default setting shows up in `part_config.retention`.
- **Done when:** Passes.
- **Needs:** -26.

## Phase B: Postgres 14 through 18 (item 4).

### 20261001-103222-18: Cover Postgres 18 syntax and report parse failures.
- **Do:** Add 18-only syntax to the corpus, such as `RETURNING OLD/NEW`, virtual generated columns, and `WITHOUT OVERLAPS`. Record which ones fail under the Postgres 17 parser. Keep a count of parse failures, along with up to N sample queries, so they're visible.
- **Red test:** Expect parse failures to show up as counted and sampled, not silently dropped.
- **Done when:** Passes. Add a note to the plan's ideas list about the `pg_query_go` v7 upgrade.
- **Needs:** -16.

## Phase C: Diffing against a snapshot (item 2).

### 20261001-103222-25: Wire diffing into the worker and stop resetting.
- **Do:** In the harvest loop:
  1. Read.
  2. Diff.
  3. Pick the top N.
  4. Fingerprint.
  5. Send.
  6. Save the snapshot.

  The window runs from the snapshot's `taken_at` to now. The worker never runs a full reset. Delete the reset code and `schema/legacy_reset.sql`, the temporary bridge from -16.
- **Baseline:** Diff on an empty or stale snapshot returns everything as new. Discard that harvest as a baseline, and add a test that pins it.
- **Stddev:** When `WindowStats` says `stddevOK=false`, record no stddev for that window instead of a misleading one.
- **Min/max:** On 17+, call `Reader.MinmaxReset` right after each harvest. If it fails, log the error and keep going, and min and max for that window come back flagged as lifetime. Drop zero-call deltas before reporting, since a window that had a reset but no calls shows min and max as 0.
- **State errors:** If the state store returns an error at runtime, such as corruption after Open, log it and treat the harvest as a baseline instead of exiting. Only `internal/state` imports SQLite, so the server must never import it.
- **Text:** Switch to `ReadStats`, then `TextCache.Retain`, then `topNDeltas`, then `TextCache.Fill`, all before any reset. Rows with QueryID 0 (hidden from the observer) get no text, so log or count them.
- **Red test:** End to end on 18:
  - `stats_reset` never changes because of the worker.
  - An outside `pg_stat_statements_reset()` mid-run gets handled per the rules.
  - After a restart, the next window starts at the saved snapshot.
- **Done when:** Passes on every supported version.
- **Needs:** -20, -21, -23, -24.

## Phase D: Rotten server (item 1).

### 20261001-103222-29: Define the protobuf API.
- **Do:** Add `proto/rotten/v1/ingest.proto` with two calls:
  - `Register(WorkerInfo) -> Registration`, which returns source IDs.
  - `SubmitHarvest(HarvestBatch) -> Ack`. A batch holds `batch_id`, the window, and repeated `FingerprintAggregate{fingerprint, normalized, contexts[], metrics, minmax_lifetime}`.

  Add `buf.yaml`, `buf generate` with connect-go, and a `make proto` target that runs `buf lint` and `buf breaking` against `master`.
- **Red test:** A round trip between a stub server and client over `httptest` (h2c and HTTP/1.1).
- **Done when:** Passes, and the generated code is committed under `gen/`.
- **Needs:** -14.

### 20261001-103222-30: Add pass key auth and the keys CLI.
- **Do:**
  - Add `rotten-server keys create|list|revoke`. It connects as `rotten_owner` (`ROTTEN_ADMIN_DSN`), since the ingest role can't create or revoke keys. `create` prints the secret once.
  - Add a Connect interceptor that checks `Authorization: Bearer`.
  - Cache key lookups with a TTL, and update `last_used_at` (throttled).
- **Grants:** `rotten_ingest` can only read `id, name, secret_hash, fqdn, revoked_at` on `api_keys`, so name those columns instead of using `select *` or `RETURNING *`.
- **Red test:** Unknown, malformed, and revoked keys all get `Unauthenticated`. A key revoked mid-connection gets rejected within the TTL. The secret never shows up in logs.
- **Done when:** Passes.
- **Needs:** -28, -29.

### 20261001-103222-31: Set up server TLS.
- **Do:** Require TLS 1.3 at minimum. Load the cert and key from files, and reload them on SIGHUP or when the files change.
- **Red test:**
  - A plaintext client gets refused.
  - A client that trusts a different CA fails.
  - After rotation, new connections see the new cert, and existing connections keep working.
- **Done when:** Passes.
- **Needs:** -29.

### 20261001-103222-32: Add the Register call.
- **Do:** Upsert `logical_sources` and `physical_sources`, moving that logic from the worker's `main`. If a key is pinned to an `fqdn`, reject any other `fqdn`.
- **Red test:** A new source gets created. An existing source gets reused. Two concurrent registrations end up with one row. A pinned key with the wrong `fqdn` gets `PermissionDenied`.
- **Done when:** Passes.
- **Needs:** -30.

### 20261001-103222-33: Build the SubmitHarvest write path.
- **Do:** Add `internal/ingest`. In one transaction, resolve fingerprint, controller, action, and job IDs (moving `identity` to the server), then insert `events` and `event_context` and record `batch_id`. Use parameterized SQL, not `Sprintf`.
- **Pruning:** Call `rotten.prune_ingested_batches()` on a timer to drop dedupe rows older than 30 days.
- **Red test:**
  - A batch produces the same rows the characterization test (-12) expects.
  - Sending the same batch twice writes one set of rows and acks both.
  - Two workers sending overlapping new fingerprints at once get no errors or duplicates.
- **Done when:** Passes.
- **Needs:** -32.

### 20261001-103222-34: Merge fingerprint_stats on the server.
- **Do:** Merge into `fingerprint_stats` for the source and for source 0 in the same transaction. Take locks in `fingerprint_id` order.
- **Red test:**
  - Results match the hand-computed values from -10.
  - Merging one batch at a time gives the same answer as accumulating first.
  - 10 concurrent workers sharing fingerprints don't deadlock.
- **Done when:** Passes.
- **Needs:** -33.

### 20261001-103222-35: Validate input and set limits.
- **Do:** Cap message size, fingerprints per batch, context entries, and string lengths. Reject a window that has `end <= start`, is more than five minutes in the future, or is longer than the max. Reject NaN or negative counters.
- **Red test:** Each limit gets rejected with `InvalidArgument`, and nothing is written.
- **Done when:** Passes.
- **Needs:** -33.

### 20261001-103222-36: Add server operations basics.
- **Do:**
  - A health endpoint that checks the database.
  - `slog` JSON logs.
  - Graceful shutdown that drains in-flight calls.
  - A config file with env var overrides.
- **Red test:** Health reports unhealthy when the database is down. SIGTERM during a call still commits or rolls back cleanly.
- **Done when:** Passes.
- **Needs:** -33.

### 20261001-103222-37: Build the worker's server client.
- **Do:** Use a Connect client with TLS (system roots or a configured CA), a bearer key from `PassKeyFile`, HTTP/2 keepalive, per-call timeouts, and exponential backoff with jitter.
- **Red test:**
  - Requests reach a test server with the right header.
  - It retries `Unavailable` but doesn't retry `Unauthenticated` or `InvalidArgument`.
  - It reconnects after the server restarts.
- **Done when:** Passes.
- **Needs:** -30, -31.

### 20261001-103222-38: Add the worker outbox.
- **Do:** Add an `outbox` table in `internal/state`. Save the batch and the next snapshot in one transaction. A sender drains the outbox oldest first and deletes each batch on ack. Cap the outbox size, and when it's full, drop the oldest batches and count them.
- **Red test:**
  - With the server down for three windows, all three windows arrive in order once it's back.
  - Killing the worker between "saved" and "acked" causes no duplicates.
  - When the cap is hit, the oldest batches get dropped and counted.
- **Done when:** Passes.
- **Needs:** -24, -37.

### 20261001-105250-1: Build the dev stack and the network layout for tests.
- **Do:** Add `dev/docker-compose.yaml` with the `observed`, `edge`, and `core` networks from `docs/plan.md`. It runs observed Postgres 18, the worker, the server (with a test CA and cert), and the rotten DB. The UI joins later, in -48. Add matching network helpers in `internal/testdb`. Explain how to use it in `dev/README.md`.
- **Red test:** A topology test checks that the worker reaches the server, the server reaches the rotten DB, and the worker can't reach the rotten DB.
- **Done when:** Passes, and `docker compose -f dev/docker-compose.yaml up` gives a working stack.
- **Needs:** -31, -36.

### 20261001-103222-39: Switch the worker over to the server.
- **Do:** Remove the rotten DB connection, `identity`, and the stats goroutines from the worker. Move to a new config format (`ServerURL`, `PassKeyFile`, `ServerCAFile`, `StateDir`, `MaxSnapshotAge`), and update `conf`.
- **Red test:** End to end on the three-network layout (-105250-1). Restart the server mid-run, and check that the rows match the expected workload with nothing lost or duplicated.
- **Done when:** Passes, and the worker binary has no rotten DB code.
- **Needs:** -25, -34, -38, -105250-1.

### 20261001-103222-40: Make the worker resilient.
- **Do:**
  - Reconnect to the observed database with backoff instead of calling `log.Fatal`.
  - On SIGTERM, finish the current harvest, flush the outbox for up to N seconds, and exit.
  - Replace the `noIdleHands` nil-map panic with a real watchdog that logs the reason and exits nonzero.
  - Keep the "sanity check fails, so exit" behavior.
- **Red test:** Restarting the observed database mid-run causes no crash, and the next window works. The sanity check returning false makes the worker exit nonzero.
- **Done when:** Passes.
- **Needs:** -39.

### 20261001-103222-41: Build release artifacts.
- **Do:** Add `make build`, which builds `rotten-worker` (cgo) and `rotten-server` (static) with version info, for Linux amd64 and arm64 plus native macOS. Add production Dockerfiles:
  - `docker/worker.Dockerfile` on Debian slim.
  - `docker/server.Dockerfile` on distroless.

  Both run as non-root, read config from a file plus env, and log to stdout. There are no platform manifests, since the deploy repo owns those.
- **Red test:** A smoke test builds both binaries and both images, then runs `--version` and `--help` in each. The server image runs `migrate` against a test DB.
- **Done when:** Passes with `make test`.
- **Needs:** -39.
## Phase E: Reports and UI (item 5).

Tasks -42 through -47 are plain SQL tested from Go, so they can run in parallel with Phase D after -26. The UI conventions are in `docs/plan.md`.

### 20261001-103222-42: Seed a report fixture dataset.
- **Do:** Add `internal/testdb.SeedReports(t, db)`. It loads a small, deterministic data set: two projects, primary and replica roles, known calls and times, contexts, and `fingerprint_stats`.
- **Red test:** Sanity checks that the seeded counts match the spec.
- **Done when:** Passes.
- **Needs:** -26.

### 20261001-103222-43: Report on top queries by call count.
- **Do:** Add `reports/top_by_calls.sql`, with parameters for source filter, time range, and limit. Fix the window filter (use `observed_window_start` and `observed_window_end`, and allow partition pruning).
- **Red test:** On the fixture, it returns the expected order, totals, and top five contexts.
- **Done when:** Passes.
- **Needs:** -42.

### 20261001-103222-44: Report on top queries by total time.
- **Do:** Add `reports/top_by_total_time.sql`.
- **Red test:** It returns the expected order on the fixture.
- **Done when:** Passes.
- **Needs:** -43.

### 20261001-103222-45: Report on queries slower than their history.
- **Do:** Add `reports/outliers.sql`. Add the missing `ORDER BY` before `LIMIT`, and define how source 0 compares with each source's own stats.
- **Red test:** The fixture's planted outlier gets returned, along with its overall mean and deviation.
- **Done when:** Passes.
- **Needs:** -43.

### 20261001-103222-46: Report on replica utilization.
- **Do:** Add `reports/replica_utilization_by_job.sql` and `..._by_controller_action.sql`. Make the role names parameters instead of hard-coding `master` and `slave`. Use full outer joins so work that only runs on one side still shows up.
- **Red test:** A job that only runs on the primary shows 100% primary. Mixed jobs show the right percentages.
- **Done when:** Passes, and `schema/example queries` is deleted, since `reports/` replaces it.
- **Needs:** -42.

### 20261001-103222-47: Report on one fingerprint over time.
- **Do:** Add `reports/fingerprint_timeseries.sql`, which buckets calls and time for one fingerprint and source. Add an index on `events (fingerprint_id, observed_window_start)` if EXPLAIN shows it's needed.
- **Red test:** The fixture's buckets match. EXPLAIN shows partition pruning.
- **Done when:** Passes.
- **Needs:** -42.

### 20261001-103222-48: Set up the UI skeleton.
- **Do:** Create a Rails 8.1 app in `ui/` on Ruby 3.4, set up like this:
  - RSpec, Capybara, FactoryBot, and Shoulda Matchers.
  - importmap, Turbo, Stimulus, and standalone Tailwind.
  - `ui/dev.Dockerfile` (with Chromium for system specs), kept separate from the production `ui/Dockerfile`.

  It connects as `rotten_ui`, owns no migrations, and uses `schema_format :sql`. The test DB gets prepared with `rotten-server migrate`. Add `make test-ui` and `make test-all`. Add the UI to the `core` network in `dev/docker-compose.yaml`.
- **Red test:** A request spec where `/up` returns 200 with the database up and 503 with it down.
- **Done when:** `make test-all` passes.
- **Needs:** -28, -105250-1.
### 20261001-103222-49: Add the users table and auth mode switch.
- **Do:**
  - Add a migration for `users(id, email citext unique, name, provider, provider_uid, password_digest null, role viewer|admin, groups text[], active, last_login_at)`, and grant it to `rotten_ui`.
  - Read `ROTTEN_UI_AUTH=oidc|password` at boot, and refuse to start if it's missing or unknown.
  - Every page requires login, except `/up` and the login pages.
  - Add `require_admin` for admin pages.
  - Inactive users get logged out on their next request.
- **Red test:**
  - Booting without `ROTTEN_UI_AUTH` fails with a clear message.
  - A logged-out request redirects to `/login`.
  - An inactive user's session gets dropped.
  - A viewer gets 403 on an admin page.
- **Done when:** Passes.
- **Needs:** -48.

### 20261001-105250-2: Add OIDC login.
- **Do:** Use `omniauth_openid_connect` with `omniauth-rails_csrf_protection`.
  - Every org-specific value comes from env, with no defaults: `OIDC_ISSUER`, `OIDC_CLIENT_ID`, `OIDC_CLIENT_SECRET`, `OIDC_GROUPS_CLAIM` (default `groups`), `ROTTEN_UI_VIEWER_GROUP`, and `ROTTEN_UI_ADMIN_GROUP`.
  - Provision users at login: match on `sub`, then on email, then create a new user. Resync name, email, and groups on every login, and derive the role from groups, failing closed.
  - Add an `OMNIAUTH_FAKE=1` offline login with a viewer persona and an admin persona, for development only.
- **Red test:** In OmniAuth test mode:
  - A callback creates a user and a session.
  - A second login with changed groups updates the role, including dropping admin.
  - With `ROTTEN_UI_VIEWER_GROUP` set, a user in neither group gets 403.
  - A missing groups claim gives a viewer at most, never an admin.
  - `OMNIAUTH_FAKE` does nothing outside development.
  - Booting in `oidc` mode with a missing `OIDC_*` value fails.
- **Done when:** Passes. Okta itself is a placeholder here: document the env vars an Okta app needs, and leave the real setup to the repo that deploys this one.
- **Needs:** -49.

### 20261001-105250-3: Add password login.
- **Do:** Use `has_secure_password` on `users`, with a `/login` form and `rate_limit` on attempts. There's no sign-up and no password reset by email. Add `bin/rails users:create[email,role]` (prints a one-time password), `users:disable[email]`, and `users:reset_password[email]`.
- **Red test:**
  - Right and wrong passwords work as expected, and the error message doesn't reveal whether the email exists.
  - The rate limit kicks in.
  - A disabled user can't log in.
  - The rake tasks do what they say.
  - In `oidc` mode, the password form and endpoint return 404.
- **Done when:** Passes.
- **Needs:** -49.

### 20261001-105250-4: Write the UI security specs.
- **Do:** Add `ui/spec/security/`. Cover:
  - CSRF on every state-changing route.
  - Session fixation: reset on login.
  - Cookie flags.
  - Security headers and CSP.
  - Host header handling.
  - Fuzzing user provisioning with nasty claim values.
  - SQL injection through report parameters.
  - Brakeman and bundler-audit, run as specs.
- **Red test:** Each spec is written to fail against a deliberately weakened config first, such as CSRF turned off.
- **Done when:** Passes.
- **Needs:** -105250-2, -105250-3.
### 20261001-103222-50: Build the report pages.
- **Do:** Add a source picker (project, environment, cluster, role) and a time range. Run the `reports/*.sql` files with bound parameters, and render sortable tables. Set a statement timeout on report queries.
- **Red test:** A system test on the fixture: pick a source and range, and see the expected rows for each report. A slow query shows a friendly timeout message.
- **Done when:** Passes.
- **Needs:** -43 through -46, -49.

### 20261001-103222-51: Build the fingerprint detail page.
- **Do:** Show the normalized SQL, a time series chart, the top contexts, and stats for each source.
- **Red test:** A system test on the fixture fingerprint.
- **Done when:** Passes.
- **Needs:** -47, -50.

### 20261001-103222-52: Build the pass key admin pages.
- **Do:** Admins can create keys (the secret shows once), list them, and revoke them, using `rotten_ui`'s narrow `api_keys` grants. Fill in `created_by` and `revoked_by`, and write each action to `ui_audit_log`, which needs a migration and grant.
- **Grants:** `rotten_ui` can't read `secret_hash`, and can only insert `name, secret_hash, fqdn, created_by`. Add a test that `INSERT ... RETURNING id` works.
- **Red test:**
  - Viewers get 403.
  - Create shows the secret once.
  - A revoked key fails server auth within the TTL (shared fixture with -30).
- **Done when:** Passes.
- **Needs:** -30, -49, -105250-4.

### 20261001-103222-53: Check report query performance.
- **Do:** Seed about 10 million events across 21 partitions. Check report latency and plans, and add indexes as needed.
- **Red test:** A benchmark-style test asserting partition pruning and a latency budget (for example, under two seconds for a three-hour range).
- **Done when:** Passes. Record the numbers in `docs/`.
- **Needs:** -50.

## Phase F: Docs.

### 20261001-103222-54: Rewrite the README and write operator docs.
- **Do:** Cover:
  - The architecture.
  - Observer grants for each version.
  - Server setup and TLS.
  - Issuing, rotating, and revoking keys.
  - Worker config.
  - UI setup, including a full env reference for both auth modes and how to point OIDC at Okta without org values in this repo.
  - The database roles and what each one may do.
  - The pg_partman permissions retention needs: the `pg_partman_bgw` role must be able to drop `rotten_owner`'s partitions.

  Update the Known Issues and TODO sections.
- **Red test:** A docs smoke check that every config key in `conf`, the server config, and every `ENV` the UI reads appears in the docs. A small Go test can do this.
- **Done when:** Passes, and someone who isn't the author can follow the setup.
- **Needs:** -41, -50.

### 20261001-113241-1: Fix the mean and stddev merge in mergeEvent.
- **Do:** `mergeEvent` adds `b.calls` to `a.calls` before it builds the first `RunningStat`, so the first side is weighted by both call counts. For two events of two calls each (means 10 and 20), it gives a mean of 13.33 instead of 15. Also, `runningstat.Init` treats the stddev as a sample stddev (`sd^2*(n-1)`), but pg_stat_statements reports a population stddev. And when `n <= 1`, `Init` stores `sd` unsquared in `m_newS` and sets `m_oldS` to 0. `Merge` reads the second side's `m_oldS`, so a one-call second event's stddev is dropped. An unsquared `sd` only reaches `Merge` on the first side. Use the original count and the right variance formula.
- **Red test:** Change the expected values in `TestMergeEventRunningStat` to the correct ones (mean 15 for the example above).
- **Done when:** Tests pass with correct pooled mean and stddev.
- **Needs:** -8.

### 20261001-113241-2: Guard the uint32 context counts.
- **Do:** The context histogram stores `uint32(calls)`. That truncates fractional values, and a call count above 4,294,967,295 overflows (out-of-range float-to-int conversion is implementation-defined in Go). Sums of counts can also wrap. Decide on a wider type or a clamp.
- **Red test:** A `mergeEvent` test with counts near the uint32 limit.
- **Done when:** Tests pass, and large counts don't wrap.
- **Needs:** -8.

### 20261001-114433-1: Remake the TLS config for observed DB fallbacks.
- **Do:** `main()` remakes the chain for rotten DB fallbacks but not for observed DB fallbacks. Those keep the pgx default config, which has no intermediates. Decide whether that's intended, and fix it if not.
- **Red test:** A config with several observed hosts gets the remade chain on every fallback.
- **Done when:** Tests pass.
- **Needs:** -11.

### 20261001-114433-2: Parse connection strings properly in `remakeSSLCertConfig`.
- **Do:** It splits on spaces and on every "=". Quoted values, values with spaces or "=", and URL-form strings (`postgres://...?sslrootcert=...`) silently produce empty paths. Use pgx parsing instead.
- **Red test:** A URL-form connection string with `sslrootcert` builds the chain.
- **Done when:** Tests pass.
- **Needs:** -11.

### 20261001-114554-1: Stop dropping stats when a fingerprint_stats flush rolls back.
- **Do:** When `reportSamples` rolls back a source (for example, only some of the 19 types exist), it still resets the in-memory stats, so the source that rolled back loses those samples. The other source already committed them. Its "Only found" log line also prints `logical_source_id` instead of the `source_id` that failed. Decide whether to keep the stats for the next pass or repair the missing rows, and fix the log line.
- **Red test:** `TestReportSamplesPartialRowsRollBack` in `stats_test.go` asserts the reset today. Flip it to the new behavior.
- **Done when:** Tests pass.
- **Needs:** -10.

### 20261001-120544-1: Flush a new fingerprint's first sample.
- **Do:** A new fingerprint's first sample can go unflushed until a second sample arrives. `reportSamples` copies `f.last` into `lastReport` when it starts. If `consumeSamples` has already recorded the first sample, no pass sees a change until another sample comes in.
- **Red test:** Let `consumeSamples` record a new fingerprint's first sample before `reportSamples` starts, step once, and expect 19 rows per source.
- **Done when:** Tests pass.
- **Needs:** -10.

### 20261001-120501-1: Make in-flight event processing deterministic.
- **Do:** In `run`, increment the processing counter (or add to a WaitGroup) before `go processEvent`, not inside it. Today the counter can read zero while spawned goroutines haven't started yet.
- **Red test:** Right after `run` spawns its goroutines, the in-flight count equals the number of unique events in the window.
- **Done when:** Tests pass, and `TestWorkerEndToEnd` can wait on the counter instead of polling the database.
- **Needs:** -13.

### 20261001-120501-2: Let `run` exit gracefully on errors and cancellation.
- **Do:** Pass `ctx` into the worker loop's database calls. Replace the `log.Fatalln` calls in `run` with returned errors, and have `main` log them and exit.
- **Red test:** Cancelling `ctx` mid-query makes `run` return promptly, and a failing sanity check makes `run` return an error instead of exiting the process.
- **Done when:** Tests pass.
- **Needs:** -12.

### 20261001-131002-1: Collapse schemas inside qualified column refs, function names, and DROP name lists.
- **Do:** Today, `public.users.id` and `users.id`, `shard_1.f()` and `shard_2.f()`, and `DROP TABLE shard_1.t` and `DROP TABLE t` each get different fingerprints, because those schemas live in String lists, not in a `Schemaname` field. Collapse them the same way as table references, unless `KeepSchemas` is set.
- **Red test:** Golden cases for each pair that share a fingerprint by default, plus schema test cases that show they differ with `KeepSchemas` on.
- **Done when:** Passes, and `make golden` shows only the intended changes.
- **Needs:** -112142-3.


### 20261001-132234-1: Fix the flaky "port 5432/tcp not found" in StartObserved.
- **Do:** `TestObserverSQL` failed once in a full `make test` run with `testdb: connection string: port "5432/tcp" not found`, then passed on rerun. Find out why `ConnectionString` runs before the port is mapped, and make `start` wait for it.
- **Red test:** Hard to force. Loop `TestObserverSQL` under `-count` until it fails, then show the fix holds for the same loop.
- **Done when:** A long loop of the testdb tests passes.
- **Needs:** none.

### 20261001-135352-2: Decide whether `<> ALL(subquery)` should group with `NOT IN (subquery)`.
- **Do:** They mean the same thing, but Postgres parses `NOT IN (subquery)` as `NOT (x = ANY (subquery))`, so its queryid differs from `<> ALL (subquery)`. Task -134632-1 leaves them apart to match Postgres. Check whether pg_stat_statements on 16 and 18 ever merges them. Group them only if it does.
- **Red test:** A cross-version case for each form.
- **Done when:** Passes.
- **Needs:** 20261001-134632-1.

### 20261001-140616-1: Decide whether nested element casts should collapse in IN lists.
- **Do:** The fingerprint ignores a single cast on an IN-list element, so `id IN (1::bigint)` groups with `id IN (1)`, though Postgres gives them different queryids. A nested cast doesn't collapse: `id IN (1::bigint::int)` and `id = ANY(ARRAY[1::bigint]::int[])` get their own fingerprint. Decide whether that split is worth fixing, and whether the single-cast merge across types is wanted.
- **Red test:** Golden cases for the nested-cast forms, with the grouping you pick.
- **Done when:** Passes, and `make golden` shows only the intended changes.
- **Needs:** 20261001-135352-1.
