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

### 20261005-150200-1: Match reports' generic plans nested-loop on a store with hundreds of sources.
- **Why (found in 20261005-123457-1):** That task's `TestPerfManySources` seeds 400 fleet logical sources besides the 12 main ones. With that many, `n_distinct(logical_source_id)` is high, so the generic plan estimates about 46 events per source where busy cluster 13 has about 35,000 in 3h. top_by_calls, top_by_total_time and outliers with a match then nested-loop the aggregate against the `matched_events` CTE (63M join-filter rows): about 2.3–2.9s at 3h, and canceled by the 15s timeout at 24h and 7d. Custom plans are fine. This predates migration 0014 (it's the same with `events_fingerprint_window`); the fleet only exposed it. The fleet was kept out of `TestPerfReports`'s seed so `make test-perf` stays green, so the suite doesn't catch it yet.
- **Do:** Make the plan independent of the per-source estimate, for example by matching through a hashed `= any(array(...))` or a semi-join the planner hashes, or by materializing the matched keys so the join can't be a nested loop over a CTE scan. Recheck 20261005-123457-2 afterwards, since it covers the same reports.
- **Needs:** nothing.
- **Red test:** Add the 400-source fleet seed to the match cases (or run them against `TestPerfManySources`'s setup) and require the 3h budget (2s) for top_by_calls, top_by_total_time and outliers with a match, in both plan cache modes.

### 20261005-150200-2: The fingerprint page's one-timeout spec flakes under load.
- **Why (found in 20261005-123457-1):** In one `make test-all` run on a loaded machine, `spec/requests/fingerprints_spec.rb:226` ("stops the page within one timeout when every query is slow") took 0.84s against its 0.65s wall-clock bound. It passed three times when run alone afterwards.
- **Do:** Keep what the spec proves (the page stops after one 300ms timeout, not one per query) without a tight wall-clock bound, e.g. by counting the queries that started, or by widening the bound to well under the slowest serial case (four queries × 0.3s).
- **Needs:** nothing.
- **Red test:** The spec, run with the CPU loaded (e.g. a busy container alongside), fails today and passes after the change.

### 20261005-123457-2: 7d match reports hit the statement timeout under heavy load.
- **Why (found in 20261004-231500-1):** In one perf run on master at load average 30–69 (other projects' containers), outliers 7d match (both plans) and top_by_total_time 7d match (generic) were canceled by the 15s statement_timeout. At load average 15–25 they take about 3.5–4.5s. Seen again in task 20261005-123457-1 on the main seed: two of three `make test-perf` runs at load average about 20–27 each had one 7d match generic case canceled (outliers, then top_by_calls) that took 3.7–5.8s in other runs; the third run, at load average 13–17, passed. They read 7 days of a cluster's events plus contexts, so a busy database could show users an error page.
- **Do:** Reproduce under controlled load (for example, run the suite with a CPU-bound container alongside), and find which part of those plans degrades most (hash spills at the default `work_mem`, the context match, parallel workers). Fix it or document the limit.
- **Needs:** nothing.
- **Red test:** A perf case that runs the 7d match reports with a concurrent load generator and requires them to finish under the UI's timeout.

### 20261005-123457-3: `TestDevObservedReplicaWaitsForAnActiveSlot` flakes under load.
- **Why (found in 20261004-231500-1):** It failed once in `make test-all` at high load average with "replica didn't log waiting for the slot", then passed alone. The slot holder is `timeout 8 pg_receivewal`, so if the replica container takes more than 8s to reach its clone, the slot is free and it never waits.
- **Do:** Hold the slot until the replica has logged that it's waiting (for example, keep `pg_receivewal` running and stop it once the log line appears), instead of for a fixed 8s.
- **Needs:** nothing.
- **Red test:** Delay the replica's start past 8s (or shorten the hold) and watch the current test fail; it must pass after the fix.

### 20261007-160000-1: `layout_spec.rb:78` flakes with a Selenium stale-node error.
- **Why (found landing 20261007-120000-1):** One `make test-all` run failed "Layout shows an admin the top bar on every page, with Admin, including the one-time pass key page" with `unhandled inspector error: Node with given id does not belong to the document` inside `visible?`. The next `make test-ui` run passed. The spec likely checks visibility of an element the page has just replaced.
- **Do:** Find the navigation or Turbo update that replaces the node and make the spec wait for the new page (e.g. assert on content of the destination page first) before checking the top bar.
- **Needs:** nothing.
- **Red test:** Reproduce by running the spec repeatedly (or under CPU load) until it fails, then show it passes the same number of runs after the fix.

## Phase F: Docs.

## Phase G: Context from pg_stat_statement_context.

Background: contexts come from the first query text pgss kept for each entry, so their counts were always skewed, and Postgres 18 drops leading comments. pssc counts calls and execution time per (userid, dbid, queryid, toplevel, tag set). Read it as counters (`calls_total`, `exec_time_total`, `stats_since` from `pg_stat_statement_context_totals`) and diff them like pgss, so the worker's interval doesn't need to match pssc's `bucket_interval`.

### 20261007-120000-4: Remove query-text context parsing and the Postgres 18 warning.
- **Do:** Delete `extractContextValue`, `serverContextKey`, the three regexes, `context_warning.go`, and the unused `internal/identity` package. Drop `ContextController`, `ContextAction`, and `ContextJob` from the worker config: fail fast with a clear message if they're present. Add an optional `ContextSchema` if pssc isn't found on the search path. At startup, log whether pssc is in use, and warn if it's loaded before pgss, if `utility_missing_queryid` keeps rising, or if `pg_stat_statement_context.extractors` can't see prepended comments (pssc's default is append-only, and production marginalia is prepended; it needs `position=any` or `position=prepend`), or if `pg_stat_statement_context.tags` leaves out a key the worker maps (pssc's default is `action, controller, job`; marginalia that uses `job_tag` needs it added). Update `dev/worker*.json` and `docs/worker.md`.
- **Needs:** 20261007-120000-3.
- **Red test:** A config with the old keys fails with the new message; a startup test logs pssc's state.

### 20261007-120000-5: Server stores real context time and the untagged context.
- **Do:** Carry each context's execution time on the wire (`QueryContext`) and store it as `attributed_time` instead of the proportional estimate. Store the untagged context (all three IDs null, or a marker, whichever reports can tell apart from a missing value). Retire `repair_context_utilization` for new data, keeping it for rows ingested before the change if needed. The worker already keeps each context's time in `QueryEvent.context_time` (from -3); ship it from there. Since -3 the worker ships the untagged context as all three IDs empty, and a tag pssc capped (`pssc.Capped`) as the literal value `(capped)`; decide whether the server stores either as a marker.
- **Needs:** 20261007-120000-3.
- **Red test:** An ingest test where two contexts with different times are stored with those times, not split by count.

### 20261007-120000-6: UI and docs for exact contexts.
- **Do:** Show the untagged context in "Top contexts" and the utilization reports, labelled clearly (for example "untagged"). Remove `CONTEXT_CAVEAT` and the "first seen" wording. Mark `docs/decisions/context-sampling.md` superseded and update the `docscheck` tests. Remove the Postgres 18 append advice from `docs/worker.md`, `docs/observed.md`, `README.md`, and `dev/README.md`, and document how to install pssc, that it's optional, and that prepended marginalia needs `pg_stat_statement_context.extractors` with `position=any` (or `prepend`), and that `pg_stat_statement_context.tags` must list `job_tag` if job marginalia uses it.
- **Needs:** 20261007-120000-5.
- **Red test:** A system spec that shows the untagged context on the fingerprint page, and one that the caveat is gone.

### 20261007-120000-7: Generic tag sets (v2).
- **Why:** pssc can keep any tag keys, not just controller, action, and job.
- **Do:** First, benchmark pssc with `tags = '*'` against the default allowlist (pssc's `bench/run.sh`), since its published numbers don't cover that. Then replace the three ID columns with a deduplicated `tagsets(id, tags jsonb unique)` table, ship a per-fingerprint top K plus an "other" row so one noisy key can't crowd out the rest, add a worker setting for which keys to ship, and make the reports and match filter work on jsonb keys. Document the advice to set `cardinality_cap`.
- **Needs:** 20261007-120000-6.
- **Red test:** A harvest with a non-default key (e.g. `route`) is stored and shown in reports.

