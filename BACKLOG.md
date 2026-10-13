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

**Decisions (user, 2026-10-07): context from pg_stat_statement_context (pssc).**
- **Source:** Read context from the pssc extension (https://github.com/benchub/pg_stat_statement_context), not from query text. Tasks 20261007-120000-1 through -6.
- **Optional:** pssc is optional on each observed database (RDS likely won't allow it). Without it the worker ships no contexts, and every call is untagged. No regex fallback: the user's marginalia is prepended on Postgres 18, so text parsing wouldn't help.
- **Untagged calls show in the UI.** Calls pgss counted that pssc didn't attribute appear as an "untagged" context, with their count and time.
- **Generic tag sets are v2.** v1 maps the `controller`, `action`, and `job` tags into today's columns. Arbitrary keys are 20261007-120000-7.

---

## Phase A: Test harness and characterization.

## Phase B: Postgres 14 through 18 (item 4).

## Phase C: Diffing against a snapshot (item 2).

## Phase D: Rotten server (item 1).

## Phase E: Reports and UI (item 5).

Tasks -42 through -47 are plain SQL tested from Go, so they can run in parallel with Phase D after -26. The UI conventions are in `docs/plan.md`.

### 20261005-123457-2: 7d match reports hit the statement timeout under heavy load.
- **Why (found in 20261004-231500-1):** In one perf run on master at load average 30–69 (other projects' containers), outliers 7d match (both plans) and top_by_total_time 7d match (generic) were canceled by the 15s statement_timeout. At load average 15–25 they take about 3.5–4.5s. Seen again in task 20261005-123457-1 on the main seed: two of three `make test-perf` runs at load average about 20–27 each had one 7d match generic case canceled (outliers, then top_by_calls) that took 3.7–5.8s in other runs; the third run, at load average 13–17, passed. They read 7 days of a cluster's events plus contexts, so a busy database could show users an error page.
- **Do:** Reproduce under controlled load (for example, run the suite with a CPU-bound container alongside), and find which part of those plans degrades most (hash spills at the default `work_mem`, the context match, parallel workers). Fix it or document the limit.
- **Needs:** nothing.
- **Red test:** A perf case that runs the 7d match reports with a concurrent load generator and requires them to finish under the UI's timeout.

### 20261012-200000-1: Landing page shows rotten stats instead of a Run reports button.
- **Why (user, 2026-10-12):** After login, the landing page (`home#show`) has one button to run reports, which duplicates the navbar.
- **Do:** Remove the button. Show:
  - ingest rate;
  - unique fingerprints tracked;
  - hosts seen in the last hour;
  - hosts seen in the last week;
  - a heat map of the last week: x is time, y is duration, and each cell's color is the number of fingerprints in that time and duration bucket. Draw it as inline SVG on the server, like the other charts (decision -51), with log-scale duration buckets.
  - First define each stat precisely (for example, "host" may mean an observed cluster or database, and "ingest rate" may be batches, events or calls per second) and say which definition you chose.
- **Caching:** These read a week of events, so don't compute them per request. Pick one, and document why:
  - a materialized view refreshed periodically (Postgres has no native auto-refresh, so it needs a refresher, such as the server or pg_cron, and `REFRESH ... CONCURRENTLY` needs a unique index);
  - or a cache shared by all UI processes. The UI's `cache_store` is `:memory_store` today, which is per process, so this means adding Solid Cache (or Redis) in the rotten DB.
  - Show how old the numbers are on the page.
- **Needs:** nothing. Measure it on the perf seed, so it stays fast at 10M rows.
- **Red test:** A system spec: after login, the landing page shows the four stats and the heat map, and has no Run reports button.

## Phase F: Docs.

## Phase G: Context from pg_stat_statement_context.

Background: contexts come from the first query text pgss kept for each entry, so their counts were always skewed, and Postgres 18 drops leading comments. pssc counts calls and execution time per (userid, dbid, queryid, toplevel, tag set). Read it as counters (`calls_total`, `exec_time_total`, `stats_since` from `pg_stat_statement_context_totals`) and diff them like pgss, so the worker's interval doesn't need to match pssc's `bucket_interval`.

### 20261012-180500-1: Unidentified internal/worker flake.
- **Why (found in 20261012-111513-1):** One `make test-all` run failed in `internal/worker`; the rerun passed. The failing test's name wasn't captured.
- **Do:** Run `go test -count=20 ./internal/worker/` (in the test container) until it fails, identify the test, and fix the race.
- **Needs:** nothing.
- **Red test:** The failing test, made to fail reliably (for example by widening the race window).

### 20261007-120000-7: Generic tag sets (v2).
- **Why:** pssc can keep any tag keys, not just controller, action, and job.
- **Do:** First, benchmark pssc with `tags = '*'` against the default allowlist (pssc's `bench/run.sh`), since its published numbers don't cover that. Then replace the three ID columns with a deduplicated `tagsets(id, tags jsonb unique)` table, ship a per-fingerprint top K plus an "other" row so one noisy key can't crowd out the rest, add a worker setting for which keys to ship, and make the reports and match filter work on jsonb keys. Document the advice to set `cardinality_cap`.
- **Needs:** 20261007-120000-6.
- **Red test:** A harvest with a non-default key (e.g. `route`) is stored and shown in reports.

