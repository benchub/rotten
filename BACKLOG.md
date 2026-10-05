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

### 20261004-221500-1: Outliers report misses short slow spells in preset ranges.
- **Decision (user, 2026-10-04):** Score each in-range window (or the worst few) against a robust baseline (median/MAD) from history, so short spikes show in preset ranges and past spells don't hide new ones.
- **Why (found in 20261004-142000-1):** `reports/outliers.sql` compares the average of a fingerprint's per-window means over the whole range with its history: every `fingerprint_stats` sample outside the range, including later ones. A 2-minute slow spell in a 1-hour or 3-hour range is averaged with 30 to 90 normal windows. And once a fingerprint has had two spells, each is in the other's history, which widens the deviation. The dev traffic's episodes therefore show only with a custom range covering one episode (dev/README.md, "Slow episodes and the outliers report"). Production spikes behave the same way.
- **Do:** Decide with the user whether that's intended. One option is scoring each in-range window (or the worst few) against history, instead of the range's average, possibly with a robust baseline (median/MAD) so past spells don't hide new ones.
- **Red test:** A report spec with 60 normal windows and 2 slow ones in a 1-hour range, plus history containing one earlier slow spell, that expects the fingerprint to be listed.

### 20261004-150000-1: Design per-call context attribution by sampling pg_stat_activity.
- **Needs:** 20261004-144107-2.
- **Why (user, 2026-10-04):** pgss credits all of an entry's calls to the context in its first text, so per-context counts can be badly wrong.
- **Do:**
  - Write a design in `docs/decisions/`, and get the user's sign-off before building anything. Cover:
    - sampling `pg_stat_activity` (`query_id`, query text) at an interval on 14+ with `compute_query_id`;
    - estimating each context's share of an entry's calls from the samples;
    - how the shares flow into `event_context` counts;
    - the bias against short queries, and the sampling rate and cost;
    - `track_activity_query_size` truncation, which can cut off trailing comments;
    - the config and proto changes.
  - Then split the build into tasks.
- **Red test:** A docscheck that the design doc exists and covers the points above.

### 20261004-161500-1: Statements the fingerprinter rejects are dropped from batches.
- **Why (found in 20261004-144107-1):** The worker fingerprints with the pinned Postgres 17 parser (pg_query_go), which rejects some valid Postgres 18 syntax, e.g. `UPDATE … RETURNING WITH (OLD AS o, NEW AS n)`. Those entries are counted and sampled as parse failures only. Their calls, time and contexts never reach the server, so reports silently undercount on 18.
- **Do:**
  - Decide with the user: wait for the pg_query_go 18 parser (see the working agreement; don't upgrade until the user confirms the release is out), or ship such entries under a fallback fingerprint, e.g. one derived from the pgss `queryid`, with a marker.
  - Meanwhile, make the gap visible: a per-window count of the calls dropped, shown in the UI or the worker status.
- **Red test:** A real-PG18 test with an unparseable statement, asserting the chosen behaviour.

## Phase F: Docs.
