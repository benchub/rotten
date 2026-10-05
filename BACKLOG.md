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

### 20261004-143600-1: Dev stack replica with its own worker, and a primary/replica traffic split.
- **Needs:** 20261004-142000-1.
- **Why (user, 2026-10-04):** The dev stack has one observed Postgres and one worker, so the role filter, per-role stats and the replica utilization reports have nothing to compare.
- **Do:**
  - **Replica:** add `observed-replica`, a Postgres 18 streaming replica of `observed-postgres` with `pg_stat_statements` preloaded.
  - **Second worker:** give the replica its own worker (`worker-replica`) with `Role: "replica"`, its own FQDN, pass key and state volume, and the same project, environment and cluster. Each worker stays on `observed` + `edge` only, as today.
  - **Worker sanity check:** use a recovery check that fits each worker, e.g. `select pg_is_in_recovery()` on the replica and `select not pg_is_in_recovery()` on the primary, so a mis-pointed worker exits.
  - **Traffic split:** extend the traffic generator with a distribution pattern:
    - writes, and some reads, run only on the primary;
    - some reporting or heavy reads run only on the replica;
    - some query shapes run on both with a set ratio (e.g. 70/30). Make the ratio vary by controller or job, so the replica utilization reports show a spread from 0% to 100%.
    - Marginalia comments stay the same on both sides.
  - **Stats function:** `pg_stat_statements` and the minmax reset function reach the replica through replication. Check that the replica worker's reset path works on a standby, or document why it doesn't need to.
  - **Docs:** `dev/README.md` and the dev section of the root `README.md`.
- **Red test:**
  - Extend the dev topology tests: the new services are on the right networks, and the replica worker reaches the server only through `edge`.
  - A generator unit test that the routing table sends each query shape to the intended targets in the intended ratios.
  - A real-Postgres test that the replica is in recovery and replays from the primary.

### 20261004-144200-1: Regex filter for report results, with highlighted matches.
- **Why (user, 2026-10-04):** People need to narrow results to queries matching a pattern, and see where the pattern matched.
- **Decisions (user, 2026-10-04):**
  - The filter runs on the server, inside the report SQL, before the top-N limit. So it's the top 50 matching rows, not a narrowing of the 50 already shown.
  - It matches the query text and the contexts (controller#action, job tag).
- **Do:**
  - **Field:** add an optional `match` field to the dataset part of the workbench form. Like the other dataset fields, it carries across reports when you switch with the chips, and the fingerprint page keeps it in its links.
  - **Matching:** case-insensitive POSIX regex (`~*`), passed only as a bound parameter. A row matches if the query text OR any of its contexts matches.
    - Utilization reports have no query text, so they match on their name column (controller#action or job).
    - The time series, which is for a single fingerprint, ignores `match` and says so.
  - **Validation:**
    - Cap the length, e.g. 200 characters.
    - Reject patterns Postgres can't compile with a clear form error and a 422, not a 500. Check them cheaply first, e.g. `SELECT '' ~* $1` in its own statement, within the existing report timeout.
    - Pathological patterns must stay bounded by the existing 15s statement timeout.
  - **Highlighting:** highlight the matches in the query text and context cells with `<mark>`.
    - Do it on the server, in a helper that HTML-escapes everything and wraps only the matched spans.
    - Ruby and Postgres regex dialects differ. If the pattern doesn't compile in Ruby, or behaves differently there, fall back to no highlight rather than wrong highlights or an error. Use a Ruby `Regexp.timeout` so highlighting can't hang.
    - Highlight inside the truncated `<details>` query disclosure too.
  - **Style:** `<mark>` is styled through the Tailwind theme with WCAG AA contrast.
  - **Docs:** update `ui/README.md` and `docs/ui.md`.
- **Red test:**
  - A request spec per report kind: the filter narrows rows before the limit. Seed more than 50 fingerprints with only a few matching, and check the matching ones beyond the top 50 show up.
  - A request spec that an invalid regex gives a 422 with a message.
  - A security spec: injection through `match`, and highlighting can't inject HTML (e.g. a pattern matching `<script>` in the query text stays escaped).
  - A helper spec for the highlighting edge cases: overlapping or empty matches, multibyte text, and a pattern that doesn't compile in Ruby.
  - A system spec: enter a pattern, run, see highlighted matches, switch reports with a chip, and the pattern is still applied.

### 20261004-144107-2: Contexts are credited by each entry's first text, not per call.
- **Decision (user, 2026-10-04):** Do both. Document the limit and add a UI caveat now. Add a separate backlog task to design sampling `pg_stat_activity` per `query_id` so contexts are credited correctly.
- **Why (found in 20261004-142000-1):** `pg_stat_statements` keeps one text per (userid, dbid, toplevel, queryid) entry, the first it saw. The worker extracts one context from that text and credits all of the entry's calls in a window to it (`buildHarvestBatchFromRows`, `extractContextValue`). A query run by many controllers is credited entirely to whichever ran it first, for the entry's lifetime, so per-context counts in the controller, action and job views can be badly wrong. The views don't say so.
- **Do:** Decide with the user. At least document the limitation where the UI shows contexts and in `docs/worker.md` (counts are "calls of entries first seen under this context"). A real fix needs another source (e.g. sampling `pg_stat_activity` query texts per queryid on 14+, where `compute_query_id` exposes `query_id`), which is a design change.
- **Red test:** Depends on the choice; for docs only, a docscheck that the caveat is present.

### 20261004-221500-1: Outliers report misses short slow spells in preset ranges.
- **Decision (user, 2026-10-04):** Score each in-range window (or the worst few) against a robust baseline (median/MAD) from history, so short spikes show in preset ranges and past spells don't hide new ones.
- **Why (found in 20261004-142000-1):** `reports/outliers.sql` compares the average of a fingerprint's per-window means over the whole range with its history: every `fingerprint_stats` sample outside the range, including later ones. A 2-minute slow spell in a 1-hour or 3-hour range is averaged with 30 to 90 normal windows. And once a fingerprint has had two spells, each is in the other's history, which widens the deviation. The dev traffic's episodes therefore show only with a custom range covering one episode (dev/README.md, "Slow episodes and the outliers report"). Production spikes behave the same way.
- **Do:** Decide with the user whether that's intended. One option is scoring each in-range window (or the worst few) against history, instead of the range's average, possibly with a robust baseline (median/MAD) so past spells don't hide new ones.
- **Red test:** A report spec with 60 normal windows and 2 slow ones in a 1-hour range, plus history containing one earlier slow spell, that expects the fingerprint to be listed.

### 20261004-163000-1: Dev services don't get SIGTERM under `go run`.
- **Why (found in 20261004-142000-1 review):** `rotten-server` (migrate and serve), `rotten-worker` and `gen-test-certs` in `dev/docker-compose.yaml` run under `go run`, which is PID 1. On SIGTERM, Docker signals only PID 1, and `go run` doesn't pass the signal on, so `docker compose stop` waits the grace period and then kills them without their graceful shutdown (the worker's final harvest, the server's drain). The `traffic` service now builds its binary and `exec`s it, and `dev/cmd/traffic`'s `TestComposeStopShutsDownCleanly` checks that.
- **Do:** Launch them the same way (`sh -ec 'go build -o /tmp/<name> ./cmd/<name> && exec /tmp/<name> ...'`), keeping their arguments.
- **Red test:** Like `TestComposeStopShutsDownCleanly`, for the worker and the server: run the compose command, send SIGTERM to the started process, and require the shutdown log line and exit 0 within 10 s.

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
