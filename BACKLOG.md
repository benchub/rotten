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

**Decisions (user, 2026-10-04).**
- **Session lifetime: 20261003-130000-3 and -150000-1.** Build these as one task, in -150000-1, and close -130000-3 as merged into it.
  - Add a per-user generation counter, `users.session_generation`. A goose migration adds the column and its grants.
  - Bump the counter on:
    - logout, which ends ALL of that user's sessions;
    - a password change or reset;
    - disabling the user;
    - an OIDC login that finds the user has lost group access.
  - Check the generation stored in the session on every request.
  - Also stamp an absolute expiry in the session: 12 hours by default, configurable through an env var documented in `docs/ui.md`. When it passes, the user must log in again. For OIDC users, that re-check picks up group changes.
- **-140000-2, login rate limits:** keep the in-process memory store. Document in `docs/ui.md` that each process keeps its own counters, so N Puma workers or replicas allow N× the limit, and recommend one UI process (or scale the limits to match).
- **-140000-1, forced password change at first login:** don't build it.

---

## Phase A: Test harness and characterization.

## Phase B: Postgres 14 through 18 (item 4).

## Phase C: Diffing against a snapshot (item 2).

## Phase D: Rotten server (item 1).

## Phase E: Reports and UI (item 5).

Tasks -42 through -47 are plain SQL tested from Go, so they can run in parallel with Phase D after -26. The UI conventions are in `docs/plan.md`.

## Phase F: Docs.

### 20261003-140000-2: Document that login rate-limit counters are per process.
- **Do:** The rate limits use a memory store in each process, so N Puma workers or replicas allow N times the limit. The user decided on 2026-10-04 to keep the memory store. In `docs/ui.md`, document:
  - that each process keeps its own counters;
  - the effective limit with N processes;
  - the recommendation to run one UI process, or to scale the limits down to match.

  If the Puma config defaults to more than one worker, say so. Also put a short comment next to the rate-limit code.
- **Red test:** A docs smoke check (`internal/docscheck`, or a spec) that `docs/ui.md` mentions the per-process limit.
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

### 20261003-200000-2: Make the worker outbox cap configurable.
- **Do:** The -38 decision said the cap is "288 by default, configurable", but `cmd/rotten-worker/main.go` passes no `OutboxCap` to `state.Open`, so it's fixed at `state.DefaultOutboxCap`. Add an optional worker config key, such as `OutboxCap`. Validate it (> 0, with a sane upper bound), keep 288 as the default, and document it in `docs/worker.md` and plan.md. This is a worker config format change, but an additive, optional one.
- **Red test:** The config parses and passes the cap through; invalid values are rejected.
- **Done when:** Passes.
- **Needs:** none.

### 20261003-200000-5: Correct plan.md's claim that the key cache TTL is configurable.
- **Do:** `docs/plan.md` around line 161 says revocation takes effect "within a configurable cache TTL". The server's key cache TTL is a fixed 30s. Either make it configurable (a server config key plus env var, documented in `docs/server.md`) or correct plan.md. This is a small decision; default to correcting the doc unless there's a reason to change the code.
- **Red test:** If code changes: a config test. If docs only: the docs smoke test still passes.
- **Done when:** Passes.
- **Needs:** none.

### 20261004-020000-1: Test flake where testdb connects to a recycled host port under heavy Docker load.
- **Do:** In the 130000-1 gate run, two `make test-all` runs were going at once. Two tests failed in `testdb: connect` after their containers were reported ready:
  - `ingest`'s `TestSubmitHarvestValidationRejectsBadInputBeforeWriting/context_string_longer_than_512_bytes` failed with `failed to receive message: unexpected EOF`, then `dial ... network is unreachable` over IPv6.
  - `reports`' `TestOutliersSkipsZeroDeviationAndMissingSourceHistory` failed with `password authentication failed for user "postgres"` on a host port. That suggests the port had been reused by another container.

  Both passed on re-run, and a single gate run is clean, so this is low priority. Investigate:
  - whether the DSN retry treats EOF and auth failures as retryable;
  - whether testdb should re-read the mapped port before each connect attempt;
  - whether testdb should verify it reached the right container, for example with a per-container password or `application_name` check.
- **Red test:** A unit test of the connect-retry classification. Or document why concurrent full gates are unsupported.
- **Done when:** Passes, or the limitation is documented in `docs/building.md`.
- **Needs:** none.

### 20261004-060000-1: Make `HasMinmaxReset` compare the major version.
- **Do:** `internal/pgss/reader.go` around line 117 checks only the minor version (`v[1] >= 11`), so a future pg_stat_statements 2.0 would skip the minmax reset. Since 200000-4, `schema/observer.sql` compares the whole version as an int array and would accept 2.0. Make the Go side agree, using `v[0] > 1 || (v[0] == 1 && v[1] >= 11)`, and handle a version with no minor part.
- **Red test:** A table test that `2.0` and `2` count as having it, `1.11` and `1.12` do, and `1.10` and `1.9` don't.
- **Done when:** Passes.
- **Needs:** none.

### 20261004-060000-2: The `ingest` test package can exceed Go's 10-minute timeout under heavy Docker load.
- **Do:** One gate run happened while a builder was running the full UI suite. testcontainers port-inspect timeouts (the 130000-1 retry path) stalled `internal/ingest`, which normally takes about 3 minutes, past `go test`'s default 10-minute timeout, and it panicked. Options:
  - share one container per package through `TestMain`;
  - set an explicit `-timeout` in `make test`;
  - cap container starts with a semaphore.
- **Red test:** A smoke check that `make test` passes an explicit `-timeout`, or that `ingest` starts at most N containers.
- **Done when:** Passes.
- **Needs:** none. Related: 20261004-020000-1.

### 20261004-060000-3: Flaky system spec `spec/system/api_keys_spec.rb:107`.
- **Do:** It failed once during the 150000-1 build with a Selenium "Node with given id does not belong to the document" error (a stale element after a Turbo re-render), then passed on re-run. Make the spec wait on a stable selector after the action, rather than holding an element reference across a re-render.
- **Red test:** Hard to reproduce. Run the spec in a loop with `UI_SPEC_ARGS`, or show that the fixed spec no longer holds element handles across navigation.
- **Done when:** Passes 10 times in a row.
- **Needs:** none.
