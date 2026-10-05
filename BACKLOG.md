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

### 20261005-020000-1: Outliers with a match filter takes 9 to 12 seconds at 7 days.
- **Why (found in 20261004-221500-1):** In the perf harness, `outliers` with `match` over a 7-day range takes about 9.5s with a custom plan and 11.4s with a generic one, close to the UI's 15s timeout. Master's SQL took about 11 and 12 seconds before the per-window scoring, so the cost isn't the scoring: it's `matched_events`, which reads every in-range `event_context` row of the source and joins controllers, actions and job tags, plus `bool_or(e.id in (...))` over every in-range event.
- **Do:** Find a cheaper way to keep groups that ran in a matching context, for example matching the controllers, actions and job tags first, then probing `event_context` by those ids, or semi-joining by (logical source, fingerprint) instead of by event id. Keep the semantics in the SQL header.
- **Red test:** Tighten the perf suite's budget for `outliers 7d match` to 5s, so it fails now.

### 20261005-020000-2: Outliers needs 30 earlier samples, which rare queries lack.
- **Decision (user, 2026-10-05):** Adaptive lookback. Look back further only until 30 samples are found, bounded to 7 days, if it stays within the perf budgets.
- **Needs:** 20261005-020000-1 (both change `reports/outliers.sql`).
- **Why (found in 20261004-221500-1):** History is the range's length before it, at least a day and at most 7 days. At 5-minute windows, a query has to run in about 30 of the 288 windows of the day before a short range, per source, to be scored. An hourly job has at most 24, so it's never an outlier in a range under about 30 hours.
- **Do:** Decide with the user whether that's fine. Options: a longer minimum lookback for short ranges, which costs time (7 days of history for a 3h range took about 2.8s in the perf harness with a draft of the query, against a 2s budget), or a lower minimum history.
- **Red test:** Depends on the choice; for example, an hourly fingerprint with a slow run in a 3h range that's listed.

### 20261004-224146-1: Built-in SQLCommenter context parsing.
- **Why (user, 2026-10-04):** SQLCommenter (OpenTelemetry) is the cross-framework standard for query context: Rails `query_log_tags` with the `:sqlcommenter` format, Django, Flask/SQLAlchemy, sqlcommenter-java, Node, Go otel, Laravel. It writes `/*key='url-encoded value',...*/` and appends it to the statement, which PG 18 keeps in `pg_stat_statements` (only leading comments are stripped). The current regex defaults only match marginalia's `key:value` format.
- **Do:** Add an optional worker config key to choose the context format: `marginalia` (today's regexes, the default), `sqlcommenter`, or `both`. For SQLCommenter, parse quoted, URL-decoded values from the last comment; map `controller`, `action`, `job` (and e.g. `route`, `framework`-specific keys, documented) onto the existing controller/action/job dimensions; ignore `traceparent`/`tracestate`. Keep the three dimensions. Document it in `docs/worker.md`, including that appended comments survive on PG 18 and prepended ones don't. Make the config change small and backward compatible.
- **Red test:** An integration test on PG 14 and 18 with SQLCommenter-formatted queries (appended, with escaped values and a `traceparent`) that are credited to the right controller/action/job.

## Phase F: Docs.
