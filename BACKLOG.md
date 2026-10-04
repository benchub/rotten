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

### 20261004-142000-1: Dev stack traffic with marginalia comments.
- **Why (user, 2026-10-04):** The dev stack runs no application-like queries, so the controller, action and job views and reports are empty, and there are few fingerprints.
- **Do:**
  - Add a `traffic` service to `dev/docker-compose.yaml`, on the `observed` network only. It creates a small made-up app schema in `observed` (courses, enrollments, favorites, users, submissions, ...), seeds it, and then runs a steady, varied load until stopped.
  - Use a mix of about 20 to 30 distinct query shapes so there's a spread of fingerprints. Include reads, writes, joins, aggregates and IN lists, with a few deliberately slow ones so the outliers report has something to show.
  - Every statement carries a leading marginalia comment in the same format as production.
    - **Web:** `/*action:list_favorite_courses,context_id:<uuid>,controller:favorites,hostname:app010001220216,pid:1546252*/ SELECT ...`
    - **Jobs:** `/*context_id:<number>,hostname:job010001045202,job_tag:Enrollment.recompute_final_score,pid:78897*/ SELECT ...`
  - Make up controllers, actions and job tags. The same query shapes should run under several contexts. Context IDs are random per request or job. Hostnames and pids come from a small pool.
  - Find out how the worker actually attributes contexts. `pg_stat_statements` keeps one text per queryid, so check whether several contexts per fingerprint can show up, and design the load so the UI shows several contexts per fingerprint where the pipeline allows it. Write down the finding.
  - Prefer a small Go program under `dev/cmd/`, as with the existing dev tools.
  - Document it in `dev/README.md` and the dev section of the root `README.md`.
- **Red test:**
  - A Go test that every generated statement's comment matches the `dev/worker.json` context regexes and gives the intended controller, action or job.
  - A real-Postgres test (`internal/testdb`) that a short generator run produces several fingerprints in `pg_stat_statements`, with comments the worker extracts.
  - The existing dev topology tests cover the new service's network isolation.

## Phase F: Docs.
