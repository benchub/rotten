# Backlog.

Work top to bottom unless a task says otherwise. Background and reasoning live in `docs/plan.md`.

**Rules.**
- Write a failing test first. Make it pass. Then refactor.
- When a task is done, cut it from this file and paste it at the bottom of `BACKLOG-COMPLETE.md`, with the date and commit SHA.
- New task IDs use the `YYYYMMDD-HHMMSS-N` format: when the task was written, plus a counter.
- Before calling a task done, run `make test-all` (or `make test` before the UI exists). This repo has no CI, so these targets are the gate.

**Decisions (user, 2026-10-02).** These settle open choices in the tasks below. A task's own text wins only where it's more specific.
- **-36, server config:** Same file format as the worker's config. `ROTTEN_SERVER_*` env vars override file values.
- **-39, worker config cutover:** Hard switch. Drop `RottenDBConn` and the other old keys, and require the new ones. If old keys are present, fail fast with a clear message.
- **-38, outbox cap:** 288 batches by default (about a day at 5-minute windows). Configurable.
- **-40, SIGTERM flush:** 10 seconds.
- **-114554-1, -120544-1, -171500-1 (worker stats-path bugs):** Skip them. Close them as superseded when -39 removes that code.
- **-143630-1, lifetime min/max:** The server skips `min_time`/`max_time` samples when `minmax_lifetime` is set. The flag already travels in the proto.
- **-143630-2, failed text fetch:** Superseded on 2026-10-03 by the "user, 2026-10-03" entry below.
- **-113241-2, context counts:** Widen to uint64/bigint end to end.
- **-143308-1, unauthenticated key lookups:** Rate-limit failed auth per client IP. DB errors stay `Unavailable`.
- **-135352-2, -140616-1, fingerprint grouping:** Match Postgres queryid grouping wherever practical. Don't merge what Postgres keeps apart.

**Decisions (user, 2026-10-03).**
- **-143630-2, failed text fetch:** Retry the fetch briefly within the harvest. If it still fails, don't advance the snapshot for the skipped entries, and carry their deltas into the next window.
- **-140616-1, IN-list element casts:** Un-merge to match Postgres. A single cast on an element splits too, so `id IN (1::bigint)` no longer groups with `id IN (1)`.
- **-50, report statement timeout:** 15s by default.
- **-51, charts:** Draw an inline SVG on the server, with no JS chart library. Tooltips and zoom come from a small Stimulus controller (20261003-080454-1).
- **-53, 10M-row performance test:** A separate opt-in target, `make test-perf`. It's not part of `make test-all`.
- **Report tasks -43 to -47** may run in parallel with Phase D.

---

## Phase A: Test harness and characterization.

## Phase B: Postgres 14 through 18 (item 4).

## Phase C: Diffing against a snapshot (item 2).

## Phase D: Rotten server (item 1).

## Phase E: Reports and UI (item 5).

Tasks -42 through -47 are plain SQL tested from Go, so they can run in parallel with Phase D after -26. The UI conventions are in `docs/plan.md`.

## Phase F: Docs.

### 20261003-120000-1: Build the UI dev image natively on arm64 if possible.
- **Do:** `make ui-image` and the compose `ui` service force `--platform linux/amd64`, so on Apple Silicon the RSpec and Chromium image runs under emulation. Find out why it was pinned (Chromium and chromedriver availability on Debian arm64?). If a native build works, use the native platform; if not, document why the pin stays.
- **Red test:** A smoke check that `make test-ui` passes with the image built for the native platform. Or, if the pin stays, a comment in the Makefile and compose file explaining it.
- **Done when:** `make test-all` passes on this arm64 host without emulation, or the pin is justified.
- **Needs:** none.

### 20261003-130000-1: Test flake where testcontainers times out inspecting the mapped port.
- **Do:** In the -105250-2 gate run, `TestWorkerDiffingOutbox/pg18` failed in `testdb` startup with `wait until ready: mapped port: retries: 30, port: "invalid port", last err: inspect ... context deadline exceeded`. Docker was too slow to answer `inspect` while the whole test suite was running in parallel. It passed when re-run. -132234-1 added `ForListeningPort` and a DSN retry, but this failure is earlier, in testcontainers' own wait strategy. Consider:
  - a longer startup timeout in `internal/testdb`;
  - retrying the whole container start once when the error is a Docker API timeout;
  - capping parallel container starts across packages with a semaphore or `-p`.
- **Red test:** It's hard to reproduce. A unit test that simulates a start error on the first try and checks that `testdb` retries once, plus a log line.
- **Done when:** Passes, and three back-to-back `make test` runs pass.
- **Needs:** none.

### 20261003-130000-2: Add a unique index on `users(provider, provider_uid)`.
- **Do:** OIDC matches users on (provider, provider_uid), but nothing in the DB enforces that pair is unique. Add a goose migration with a partial unique index where `provider_uid IS NOT NULL`. Handle `RecordNotUnique` in OidcLogin's create path, which is already retried.
- **Red test:** A Go migrate test that a duplicate (provider, provider_uid) is rejected, plus a Rails spec that a concurrent duplicate create is retried and doesn't become a 500.
- **Done when:** Passes.
- **Needs:** none.

### 20261003-130000-3: Revoke sessions when OIDC group membership is lost.
- **Do:** A demotion or removal from the groups only takes effect at the user's next login. Decide whether to cap the session's lifetime (for example, re-authenticating after N hours) or re-check periodically. This needs a decision from the user; ask before building.
- **Red test:** Depends on the decision.
- **Done when:** Passes.
- **Needs:** -105250-4.

### 20261003-140000-1: Let users change their own password, and add `users:enable`.
- **Do:**
  - Add a page where a logged-in password user changes their password, using the current one. The fingerprint check already ends their other sessions; re-fingerprint the current session.
  - Add a `users:enable[email]` rake task.
  - Optionally, force a change at first login. That needs a goose migration (`must_change_password`); ask the user first.
- **Red test:**
  - Changing the password works, the current session stays alive, and other sessions are dropped.
  - A wrong current password is rejected.
  - `users:enable` works.
- **Done when:** Passes.
- **Needs:** -105250-4.

### 20261003-140000-2: Share login rate-limit counters across UI processes.
- **Do:** The rate limits use a memory store in each process, so N Puma workers or replicas allow N times the limit. Pick a shared store (Solid Cache in the rotten DB would need a goose migration and grants), or document a single-process deployment. Ask the user.
- **Red test:** Depends on the decision.
- **Done when:** Passes.
- **Needs:** none.

### 20261003-150000-1: Make logout and expiry revoke stolen session cookies.
- **Do:** With the cookie session store, a copy of the cookie taken before logout still works afterwards. Sessions also have no expiry. Two `pending` specs in `ui/spec/security/session_fixation_spec.rb` lock in this gap. Options:
  - A server-side session store, which would need a goose migration and grants.
  - A per-user session generation counter in `users`, bumped on logout.
  - An absolute expiry stamped in the session.

  Decide together with 20261003-130000-3, since both are about session lifetime. Ask the user.
- **Red test:** Un-pend the two specs, and add an expiry spec.
- **Done when:** Passes.
- **Needs:** none.

### 20261003-150000-2: Cover the dev-only fake OIDC login route in the CSRF spec.
- **Do:** `ui/spec/security/csrf_spec.rb` enumerates routes from the test environment, so the `OMNIAUTH_FAKE=1` dev route isn't covered. Add a spec that boots with the fake enabled, or assert that the route can't exist in production.
- **Red test:** A fake route that skips CSRF fails the spec.
- **Done when:** Passes.
- **Needs:** none.

### 20261003-160000-1: Narrow the report source picker as the user chooses.
- **Do:** Project, environment, cluster and role are independent dropdowns, so the user can pick a combination that doesn't exist. They only find out when they run the report. Add a small Stimulus controller, or a server-rendered cascade, that narrows each dropdown to existing combinations. Keep it CSP-compliant.
- **Red test:** A system spec where picking a project limits the environment options to that project's environments.
- **Done when:** Passes.
- **Needs:** none.

### 20261003-170000-1: Run the fingerprint page's queries under one timeout.
- **Do:** `/fingerprints/:id` runs three report queries: the time series, the contexts and the sources. Each runs in its own read-only transaction with its own `statement_timeout`, so the worst case is about 3 × `ROTTEN_UI_REPORT_TIMEOUT`. Run them in one transaction with a single deadline, or set a page-level budget.
- **Red test:** With a tiny timeout and a slow query, the page fails within about one timeout, not three.
- **Done when:** Passes.
- **Needs:** none.

### 20261003-170000-2: Show an all-sources row in the fingerprint stats table.
- **Do:** The stats table covers only the selected project, environment, cluster and role. Add an "all sources" row, using the lifetime `fingerprint_stats` data or an aggregate across all sources, so operators see the fingerprint's overall footprint.
- **Red test:** A system spec on the fixture shows the all-sources totals.
- **Done when:** Passes.
- **Needs:** none.

### 20261003-180000-1: Add an audit log viewer, and audit user admin actions.
- **Do:** -52 added `ui_audit_log`, which `rotten_ui` can insert into and read but not change. Do two things:
  - Add an admin-only, paginated page for reading it.
  - Write audit rows for user admin actions too: the `users:*` rake tasks, and OIDC role changes at login if wanted.
- **Red test:**
  - Viewers get 403.
  - Admins see the entries newest first.
  - `users:disable` writes an audit row.
- **Done when:** Passes.
- **Needs:** none.

### 20261003-190000-1: Make replica utilization scale past 7 days on busy clusters.
- **Do:** After the -53 rewrite, `replica_utilization_by_controller_action` takes about 5.5s at 7d on the 10M-event perf seed (by_job about 3.2s). Cost grows linearly with the cluster's events in the range, so a busier cluster or a custom range up to 21 days could hit the 15s UI timeout. Consider a structural fix, such as storing each event's context total at ingest (a goose migration plus an ingest change), so the report doesn't recount all `event_context` rows.
- **Red test:** A `make test-perf` case at 21d, or at 7d with a heavier seed, that stays within the UI timeout.
- **Done when:** Passes.
- **Needs:** none.

### 20261003-200000-1: Fix the `serve` flag help text about config precedence.
- **Do:** In `cmd/rotten-server/serve.go` around lines 221–230, the flag help says "default config X, then $ENV". `loadServeConfig` actually applies the flag, then the `ROTTEN_SERVER_*` env var, then the config file, then the default. Make the help text match, as `docs/server.md` already does.
- **Red test:** A test that the help output states the actual precedence.
- **Done when:** Passes.
- **Needs:** none.

### 20261003-200000-2: Make the worker outbox cap configurable.
- **Do:** The -38 decision said the cap is "288 by default, configurable", but `cmd/rotten-worker/main.go` passes no `OutboxCap` to `state.Open`, so it's fixed at `state.DefaultOutboxCap`. Add an optional worker config key, such as `OutboxCap`. Validate it (> 0, with a sane upper bound), keep 288 as the default, and document it in `docs/worker.md` and plan.md. This is a worker config format change, but an additive, optional one.
- **Red test:** The config parses and passes the cap through; invalid values are rejected.
- **Done when:** Passes.
- **Needs:** none.

### 20261003-200000-3: Validate `--fqdn` in `rotten-server keys create`.
- **Do:** `internal/auth/admin.go` `CreateKey` stores the FQDN exactly as typed. The UI (-52) validates it. Apply the same rules in the CLI, normalizing to lowercase with no trailing dot, so CLI and UI keys behave the same. Update `docs/keys.md`.
- **Red test:** Invalid FQDNs are rejected; mixed case and a trailing dot are normalized.
- **Done when:** Passes.
- **Needs:** none.

### 20261003-200000-4: Check the pg_stat_statements extension version in `observer.sql` on PG17+.
- **Do:** On PG17 and later, `schema/observer.sql` wraps the 4-argument `pg_stat_statements_reset`, which needs extension version ≥ 1.11. After a `pg_upgrade`, the extension can still be at 1.10, and the script fails with an unclear error. Add a precondition that raises a clear "run `ALTER EXTENSION pg_stat_statements UPDATE`" message. Alternatively, gate on `extversion` rather than `server_version_num`.
- **Red test:** On PG17 with the extension at 1.10, if testdb can install that version, the script fails with the clear message. Otherwise, unit-test the gating logic.
- **Done when:** Passes.
- **Needs:** none.

### 20261003-200000-5: Correct plan.md's claim that the key cache TTL is configurable.
- **Do:** `docs/plan.md` around line 161 says revocation takes effect "within a configurable cache TTL". The server's key cache TTL is a fixed 30s. Either make it configurable (a server config key plus env var, documented in `docs/server.md`) or correct plan.md. This is a small decision; default to correcting the doc unless there's a reason to change the code.
- **Red test:** If code changes: a config test. If docs only: the docs smoke test still passes.
- **Done when:** Passes.
- **Needs:** none.
