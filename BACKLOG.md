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

### 20261016-104605-1: Register stores the worker's FQDN unnormalized.
- **Why (found in 20261016-102622-1 review):** `Register` (`internal/ingest/register.go:85-97`, upsert at `:133-137`) writes the raw `src.FQDN` into `rotten.physical_sources`. Pins match ignoring case and a trailing dot, so `DB1.example.com`, `db1.example.com.` and `db1.example.com` authenticate as the same host but become three physical sources, splitting that host's reports. Suffix pins widen this from one host's spellings to a whole domain's.
- **Do:** Store the normalized FQDN (lowercase, no trailing dot) in `Register`, for every pin type. Decide what happens to existing rows with other spellings: a migration that merges them, or leave them and document it. Check whether the worker's own registration identity (`internal/state`, `cmd/rotten-worker`) compares FQDNs and would see a normalized value as a change of identity, which drops the outbox (`docs/worker.md`).
- **Needs:** the user's call on existing rows.
- **Red test:** registering as `DB1.Example.COM.` and as `db1.example.com` yields one physical source.

### 20261016-161500-1: Don't send lifetime totals for entries the snapshot didn't have.
- **Why (user, 2026-10-16):** On canvas-c20, after `pg_read_all_stats` was granted mid-run, about 620 previously hidden entries became visible. `pgss.Diff` rule 4 (`internal/pgss/diff.go:158`, "Not in the snapshot: new") sent each one's lifetime counters as one window's activity. `SAVEPOINT $1` alone reported 67,286,178 calls in a single window, which wrecks trending. The same thing happens to any entry that was missing from the previous snapshot for any reason other than being created during the window. The worker already treats its first harvest as a baseline; this is the per-entry version of that.
- **Do:** For an entry not in the previous snapshot (rule 4 only):
  - 17+: if `stats_since` is at or after the previous snapshot's time (allow for clock skew the way `pssc_contexts.go:83` does), it really is new, so count it. Otherwise, save it in the next snapshot and send nothing for it this window.
  - 14-16 (no `stats_since`): always save it and send nothing. That gives up the first window of each new query, which the user accepted ("throw away the first read").
  - Rules 1-3 (global reset, recreated, counters went down) stay as they are.
  - Log one line per harvest with the count of entries held as baselines, not one line per entry.
  - Check how this interacts with `carrySkippedTextSnapshot`/`zeroCarryBaseline` and the pssc untagged path, so contexts and pgss agree on which calls count.
  - Update the comment on `Diff` and `docs/worker.md`.
- **Red test:** a snapshot without key K, then a harvest where K has 1,000,000 calls and `stats_since` before the previous snapshot, sends no delta for K and keeps K in the next snapshot. The harvest after that sends only the growth. On 17+, an entry with `stats_since` after the previous snapshot is still sent in full. On 14-16, any entry missing from the snapshot sends nothing on its first appearance.

### 20261016-170000-1: Multi-select source filters with chips.
- **Why (user, 2026-10-16):** With several clusters reporting, the Project, Environment and Cluster dropdowns (`ui/app/views/reports/_source_fields.html.erb`, `source_picker_controller.js`) force one value each. Only Role has "All roles".
- **Do:** Each of Project, Environment, Cluster and Role gets an "All" option and a chip field below its dropdown.
  - Picking a specific value adds it as a chip (no duplicates).
  - Each chip has its own remove button.
  - Picking "All" empties that field's chips.
  - An empty field means all values.
  - Values in a field are OR'd. Fields are AND'd with each other.
  - The dropdowns keep narrowing each other the way the source picker does now. A chip that no longer exists under the other filters is shown as stale and ignored, not silently dropped.
  - Carry the selection in the URL as repeated params (`project[]=...`), so links and the form still round-trip.
  - Update `ReportQuery` validation and every report's SQL to filter on arrays (`= ANY($n)`), still as bound parameters.
  - Reports that need exactly one role, such as replica utilization with its primary and replica roles, keep their own fields.
  - Works without JS: a plain multi-select fallback is fine.
- **Red test:** a system spec picks two clusters as chips, removes one, then picks "All", and the report rows follow each step. A request spec shows `cluster[]=a&cluster[]=b` returning both clusters' rows and no others.

### 20261016-170000-2: Several match patterns, plus exclude patterns.
- **Why (user, 2026-10-16):** One Match regex isn't enough. The user wants several matches OR'd together, and a list of exclude regexes.
- **Do:** Turn Match into a chip field of patterns, OR'd together, and add an Exclude chip field.
  - Excludes are applied first, then matches filter what's left.
  - Overlap between them is the user's concern; don't warn about it.
  - Validate every pattern with `match_compiles?`, and say which pattern is invalid.
  - Keep `MAX_MATCH_LENGTH` per pattern and cap the number of patterns (pick a cap and document it).
  - URL params are `match[]` and `exclude[]`. Accept the old single `match=` so existing links still work.
  - Like Match, Exclude only applies to reports where `Report#matches?` holds.
- **Red test:** with fingerprints A, B and C, matches A and B plus an exclude matching B return only A.

### 20261016-170000-3: Click a context to filter on it; ignore untagged.
- **Why (user, 2026-10-16):** In the context lists, like "wiki_pages_api#show_revision (33)", the user wants to click one and restrict every report to calls in that context. Untagged calls are noise in most views.
- **Do:**
  - Make each context entry a link that sets a context filter carried across all reports, not just the time series that has `context` today (`ReportQuery::CONTEXT_FORMAT`).
  - With a context filter set, reports count only the calls and time attributed to that context, using the pssc counts, not every call of a fingerprint that has that context somewhere.
  - Show the active filter with a remove control.
  - Add an "Ignore untagged context" checkbox that drops untagged (`##`) calls from context lists and from report totals. The page must say that totals then leave out untagged calls.
  - Decide and document whether the context filter is a single value or a chip list. A chip list matches the other filters and is preferred.
- **Red test:** clicking a context on a report page reloads it with only fingerprints that have calls in that context, and their counts equal that context's calls. With "Ignore untagged" checked, a fingerprint whose only calls are untagged disappears.

## Phase E: Reports and UI (item 5).

Tasks -42 through -47 are plain SQL tested from Go, so they can run in parallel with Phase D after -26. The UI conventions are in `docs/plan.md`.

## Phase F: Docs.

## Phase G: Context from pg_stat_statement_context.

Background: contexts come from the first query text pgss kept for each entry, so their counts were always skewed, and Postgres 18 drops leading comments. pssc counts calls and execution time per (userid, dbid, queryid, toplevel, tag set). Read it as counters (`calls_total`, `exec_time_total`, `stats_since` from `pg_stat_statement_context_totals`) and diff them like pgss, so the worker's interval doesn't need to match pssc's `bucket_interval`.

### 20261012-180500-1: Unidentified internal/worker flake.
- **Why (found in 20261012-111513-1):** One `make test-all` run failed in `internal/worker`; the rerun passed. The failing test's name wasn't captured.
- **Tried (2026-10-13):** 200 serial package runs with `-race -v` (about 9.5h) all passed. The fixed-folder collision above doesn't explain it, since the other concurrent run was in a different worktree. Unverified suspects: the watchdog tests in `internal/worker/parse_failures_test.go` (`TestNoIdleHandsWatchdogDoesNotFireOnHealthyRunLoop`, `...DuringReconnectBackoffCap`), which use fixed 4s sleeps with a 10ms margin and may fail under heavy load.
- **Do:** Next time it fails, capture the test name from the log (the gate log keeps it). Then make it fail reliably and fix it.
- **Needs:** a captured failure.
- **Red test:** The failing test, made to fail reliably.

### 20261007-120000-7: Generic tag sets (v2).
- **Why:** pssc can keep any tag keys, not just controller, action, and job.
- **Do:** First, benchmark pssc with `tags = '*'` against the default allowlist (pssc's `bench/run.sh`), since its published numbers don't cover that. Then replace the three ID columns with a deduplicated `tagsets(id, tags jsonb unique)` table, ship a per-fingerprint top K plus an "other" row so one noisy key can't crowd out the rest, add a worker setting for which keys to ship, and make the reports and match filter work on jsonb keys. Document the advice to set `cardinality_cap`.
- **Needs:** 20261007-120000-6.
- **Red test:** A harvest with a non-default key (e.g. `route`) is stored and shown in reports.

