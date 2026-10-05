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

### 20261004-231500-1: Precompute per-query sample counts for outliers history.
- **Why (found in 20261005-020000-2):** With adaptive lookback, a 3h outliers range has about 17k of 18k groups short of 30 samples in the default day, so the report reads about 955k older rows (about 2.5s; the outliers budget was raised to 10s). Precomputed per-(logical source, fingerprint) window counts or recent-window summaries would let the report find each group's last 30 windows without scanning.
- **Do:** Measure first. Options: a rollup table maintained at ingest (per group, per day: window count, and enough to pick the last 30 windows), or a covering index on `events (logical_source_id, fingerprint_id, observed_window_start)` (measured at about 2.7s for the per-group LATERAL version, with slower inserts and more disk). Keep results identical.
- **Needs:** 20261005-020000-2.
- **Red test:** Tighten the perf suite's outliers 3h budget back to 2s, so it fails now.

### 20261005-123457-1: Decide whether to drop `events_fingerprint_window`.
- **Why (found in 20261004-231500-1):** Migration 0013's `events_source_fingerprint_window` on `(logical_source_id, fingerprint_id, observed_window_start)` serves the per-fingerprint reports about as well as 0008's `(fingerprint_id, observed_window_start)` on the perf data: e.g. fingerprint_timeseries 21d typical 3ms / 1ms with only the new index, 3ms / 1ms with both. Dropping 0008's index would save 392 MB per 10M events and one btree insert per event (about 10–30% of insert time, see docs/perf.md).
- **Do:** List every query that uses `events_fingerprint_window` (the per-fingerprint reports, outliers' worst-window lookup, the UI), and check each with only the new index, including a fingerprint on many sources and Postgres 14–17, which have no btree skip scan. If none regresses, add a migration that drops it, and change the perf suite's index decision to require the new index instead. Note the lock in docs/database.md.
- **Needs:** 20261004-231500-1.
- **Red test:** The perf suite fails if `events_fingerprint_window` exists while a per-fingerprint case is no slower without it, or, in the other direction, a case that needs it fails its budget once it's dropped.

### 20261005-123457-2: 7d match reports hit the statement timeout under heavy load.
- **Why (found in 20261004-231500-1):** In one perf run on master at load average 30–69 (other projects' containers), outliers 7d match (both plans) and top_by_total_time 7d match (generic) were canceled by the 15s statement_timeout. At load average 15–25 they take about 3.5–4.5s. They read 7 days of a cluster's events plus contexts, so a busy database could show users an error page.
- **Do:** Reproduce under controlled load (for example, run the suite with a CPU-bound container alongside), and find which part of those plans degrades most (hash spills at the default `work_mem`, the context match, parallel workers). Fix it or document the limit.
- **Needs:** nothing.
- **Red test:** A perf case that runs the 7d match reports with a concurrent load generator and requires them to finish under the UI's timeout.

### 20261005-123457-3: `TestDevObservedReplicaWaitsForAnActiveSlot` flakes under load.
- **Why (found in 20261004-231500-1):** It failed once in `make test-all` at high load average with "replica didn't log waiting for the slot", then passed alone. The slot holder is `timeout 8 pg_receivewal`, so if the replica container takes more than 8s to reach its clone, the slot is free and it never waits.
- **Do:** Hold the slot until the replica has logged that it's waiting (for example, keep `pg_receivewal` running and stop it once the log line appears), instead of for a fixed 8s.
- **Needs:** nothing.
- **Red test:** Delay the replica's start past 8s (or shorten the hold) and watch the current test fail; it must pass after the fix.

## Phase F: Docs.
