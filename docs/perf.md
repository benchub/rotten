# Report query performance

This page covers how the report SQL in `reports/*.sql` performs against a realistically sized `rotten` database: about 10 million events across 21 daily partitions. It records latency, partition pruning, plan shapes, the `events (fingerprint_id, observed_window_start)` index decision and its reversal (migration 0014), and the replica_utilization rewrite. The suite is `reports/perf_test.go` (task 20261001-103222-53).

## Reproducing

```sh
make test-perf                                  # about 10M events, twice; around 30 minutes
make test-perf ROTTEN_PERF_EVENTS=500000        # quick smoke run
make test-perf PERF_TEST_ARGS='-run TestPerfReports'
make test-perf PERF_TEST_ARGS='-run TestPerfManySources'
```

- **Two tests:** `TestPerfReports` runs every report on the main seed below. `TestPerfManySources` seeds the same data plus the fleet (400 more logical sources) in its own database, and runs only `fingerprint_all_sources` and the index decision for it (see "Dropping `events_fingerprint_window`").

- **What runs:** `make test-perf` runs `go test -tags perf ./reports` in the test image.
- **Not in the gate:** the `perf` build tag keeps the suite out of `make test`, `make test-unit` and `make test-all`. `make test` does run `go vet -tags perf ./reports`, so a compile break in the suite still fails the gate.
- **PG18 only:** the suite uses `internal/testdb` with a real Postgres in Docker, the rotten database image (`rotten-db-test:18`). The PG14–18 matrix applies to observed databases, not to the rotten database.
- **Output:** the results tables are printed with `-v`.

### What it does

1. **Seeds the data on the server side.** It uses `INSERT ... SELECT generate_series` over 6 parallel connections, one day at a time, then runs `VACUUM ANALYZE`.
   - **Determinism:** each day's insert runs `select setseed(...)` first, on the same connection, with a seed derived from the day's index. Contexts are derived from the event's natural key, not its id. The data ends at a fixed anchor, 2026-01-21 12:00 UTC. So every run seeds the same data: 10,039,225 events and 13,052,198 event_context rows (10,432,825 and 13,563,878 with the fleet). Every 3h range sits in one partition, and every 24h range spans two. Event ids and physical row order can still vary with scheduling.
   - **Sources:** 12 streams. Canvas clusters 13, 7 and 21 and bridge cluster 1 each have a primary with one host and a replica with two hosts. Cluster 13 carries 4× the weight of each other cluster. The reports query cluster 13.
   - **Fleet (`TestPerfManySources` only):** 400 more logical sources (project `fleet`, one cluster and one host each), added in task 20261005-123457-1 so the index that leads with the source has hundreds of distinct leading values in every partition. It's kept out of `TestPerfReports`'s seed because it skews the source-wide reports' generic plans (see "Known limits"). Each harvests hourly, deterministically: fingerprint 20001 (on all 400, the "many" case) and one of a pool of 500 more. That's 393,600 events.
   - **Windows:** 5 minutes long, spanning 21 days, so 21 populated daily partitions created by `public.create_partition_time`.
   - **Fingerprints:** pools of 15,000 for canvas and 5,000 for bridge. Those with `id % 1000 = 7` (20 of them) are marked unparsed, for `unparsed_summary`. Fingerprints are picked with a skew (`pool*random()^2`), and calls are skewed too (`50000/idx`). On cluster 13 over 21 days, the hot fingerprint (id 1) has 16,549 events and the typical fingerprint (id 200) has 1,577.
   - **Outliers:** a slice of fingerprints is 10× slower in the last 2 hours, so `outliers` has something to find.
   - **event_context:** about 1.3 rows per event, 20% of them job contexts. Each row gets its event's `logical_source_id` and an equal share of its time as `attributed_time`, as ingest would write them (every context of a seeded event has the same `c`).
   - **fingerprint_stats:** `mean_time` rows per source and for source 0.

   Seeding takes 1m20s–4m20s, depending on the machine's load.
2. **Runs every report the way the UI does.**
   - It connects as `rotten_ui` and uses `PREPARE` / `EXECUTE` with bound parameters.
   - Each run is in a read-only transaction with `statement_timeout = 15s` and `jit = off`, both transaction-local, as in the UI's `ReportRunner`. `beginReportTx` in `reports/report_tx_test.go` sets them up, and `TestBeginReportTxSettings` (in `make test`) checks them.
   - Each case runs under both `plan_cache_mode = force_custom_plan` and `force_generic_plan`. Rails reuses prepared statements, so after five executions Postgres may switch to a generic plan, where pruning happens at executor startup or per loop.
   - Each case gets one warmup, then the median of 5 runs (3 for the source-wide 24h and 7d cases).
   - A run canceled by the timeout is reported as a failure, and the suite carries on.
3. **Asserts pruning and latency.**
   - **Pruning:** `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` must scan only partitions that overlap the range, and at least one events or event_context partition (replica_utilization reads only event_context). A partition counts as scanned when its node has loops > 0. Generic plans report pruned partitions as "Subplans Removed".
   - **Latency budgets** (median):

     | Reports | Ranges | Budget |
     |---|---|---|
     | Every report but outliers | 3h | 2s |
     | The fingerprint reports, and in `TestPerfManySources` fingerprint_all_sources, including a fingerprint on all 400 fleet sources | 3h, 7d, 21d | 2s |
     | top_by_calls, top_by_total_time, unparsed_summary, both replica_utilization reports | 24h and 7d | 15s |
     | outliers, with and without a role or a match | 3h | 2s |
     | outliers, with and without a role or a match | 24h and 7d | 10s |
     | Both replica_utilization reports | 21d | 15s |

     15s is the UI's `statement_timeout`, so it's the hard failure line: past it, the user gets an error page instead of a report. It's a ceiling, not a target. The 7d numbers below show how much headroom each report has. outliers had 10s at every range (user decision, 2026-10-04, task 20261005-020000-2): its adaptive lookback read up to 7 days of history for rare fingerprints even at 3h (see "Adaptive lookback" below). Migration 0013's index brought 3h back under 2s (task 20261004-231500-1, see "Outliers history index" below), so 3h is 2s again. 24h and 7d stay at 10s. The UI shows a busy indicator while a report runs, so the wait is visible.
4. **Decides on the indexes.** Both tests do this, each on its own cases.
   - The test fails if `events_source_fingerprint_window` (migration 0013) is missing.
   - It logs index sizes. `TestPerfReports` also times a one-hour insert and the build (each migration's own Up section, rolled back) for both `events_source_fingerprint_window` and migration 0008's `events_fingerprint_window`.
   - **Migration 0008's index.** In a transaction it rolls back, it toggles `events_fingerprint_window`: it drops it if the schema has it, or builds it with migration 0008's Up section if it doesn't (the schema since migration 0014). It reruns every report that reads events by fingerprint (the per-fingerprint reports, unparsed_summary and outliers) in that transaction before and after the toggle, and compares the two. A report *needs* the index if it's more than 2× **and** more than 25ms slower without it, or times out only without it. The test fails if the index exists and no report needs it (it costs an index entry per event), or if it doesn't exist and a report needs it, or, when it exists, if a report is over its budget without it.
   - **Migration 0013's index.** Still in that transaction, with `events_fingerprint_window` gone, it drops `events_source_fingerprint_window` too and reruns the fingerprint reports with no index on the fingerprint. It requires `events_source_fingerprint_window` to be at least 2× faster for the typical fingerprint at 7d and 21d.

The UI also offers a 6h preset and custom ranges up to 31 days. Those sit between, or beyond, the measured presets. Retention is 21 days, so a 31-day range covers at most 21 partitions.

### Caveats

- **Hardware:** one machine, an Apple M1 Max (10 cores, 32 GB) running Docker Desktop with 10 CPUs and 7.9 GB of memory.
- **Postgres:** 18.4 with its default config (`shared_buffers` 128 MB, so most "reads" come from the OS cache).
- **Caches:** they're warm after the warmup run.
- **Your numbers will differ.** Treat these numbers as relative evidence and an order of magnitude, not an SLA.

## Results

These are medians from the final run, with the index present. "Scanned" means partitions scanned out of the partitions that overlap the range. "Removed" is the Subplans Removed count in the generic plan, summed over all Append nodes. The database has more partitions than the 21 seeded ones, because pg_partman premakes partitions around the current date.

### Source-wide reports (canvas, production, cluster 13)

| Report | Range | Custom | Generic | Scanned | Removed (generic) |
|---|---|---|---|---|---|
| top_by_calls | 3h | 32ms | 84ms | 1/1 | 90 |
| top_by_calls role=replica | 3h | 22ms | 70ms | 1/1 | 90 |
| top_by_calls match | 3h | 48ms | 102ms | 1/1 | 90 |
| top_by_total_time | 3h | 31ms | 83ms | 1/1 | 90 |
| top_by_total_time match | 3h | 46ms | 100ms | 1/1 | 90 |
| outliers | 3h | 976ms | 898ms | 8/8 | 203 |
| outliers role=primary | 3h | 283ms | 345ms | 8/8 | 203 |
| outliers match | 3h | 133ms | 252ms | 8/8 | 203 |
| replica_utilization_by_controller_action | 3h | 16ms | 28ms | 1/1 | 30 |
| replica_utilization_by_job | 3h | 6ms | 7ms | 1/1 | 30 |
| top_by_calls | 24h | 175ms | 371ms | 2/2 | 87 |
| top_by_calls role=replica | 24h | 125ms | 263ms | 2/2 | 87 |
| top_by_calls match | 24h | 244ms | 452ms | 2/2 | 87 |
| top_by_total_time | 24h | 171ms | 355ms | 2/2 | 87 |
| top_by_total_time match | 24h | 253ms | 462ms | 2/2 | 87 |
| outliers | 24h | 1.53s | 1.81s | 9/9 | 198 |
| outliers role=primary | 24h | 473ms | 507ms | 9/9 | 198 |
| outliers match | 24h | 905ms | 766ms | 9/9 | 198 |
| replica_utilization_by_controller_action | 24h | 74ms | 207ms | 2/2 | 29 |
| replica_utilization_by_job | 24h | 36ms | 58ms | 2/2 | 29 |
| top_by_calls | 7d | 2.05s | 3.21s | 8/8 | 69 |
| top_by_calls role=replica | 7d | 1.39s | 2.10s | 8/8 | 69 |
| top_by_calls match | 7d | 3.49s | 3.87s | 8/8 | 69 |
| top_by_total_time | 7d | 2.09s | 3.24s | 8/8 | 69 |
| top_by_total_time match | 7d | 3.64s | 3.74s | 8/8 | 69 |
| outliers | 7d | 4.51s | 4.36s | 15/15 | 168 |
| outliers role=primary | 7d | 879ms | 1.28s | 15/15 | 168 |
| outliers match | 7d | 3.81s | 4.16s | 15/15 | 168 |
| replica_utilization_by_controller_action | 7d | 517ms | 1.88s | 8/8 | 23 |
| replica_utilization_by_job | 7d | 269ms | 548ms | 8/8 | 23 |
| replica_utilization_by_controller_action | 21d | 2.09s | **5.42s** | 21/21 | 10 |
| replica_utilization_by_job | 21d | 967ms | 1.71s | 21/21 | 10 |

The replica_utilization rows are from task 20261003-190000-1's final run, after migration 0011. Since they read only `event_context`, their Scanned and Removed columns count event_context partitions. The top_by_calls and top_by_total_time rows are from task 20261005-020000-1's final run, which rewrote their match filter. The machine was busier than in earlier runs (load average 7–17 from other containers), so compare those rows with the interleaved before-and-after numbers under "Match filter" below rather than with older rows. More partitions are premade now, so Removed counts are higher. The outliers rows are from task 20261004-231500-1's final run (load average about 15–20), which added migration 0013 for the adaptive lookback's history. Compare them with the interleaved numbers under "Outliers history index" below. The outliers Scanned column counts events partitions, including the history before the range: 7 days before it, since a fingerprint short of samples can look back that far.

Plan shapes:

- **top_by_calls and top_by_total_time:**
  - Custom plans use a bitmap scan on `(logical_source_id, calls)` up to 24h, then a seq scan of the 8 partitions at 7d.
  - Generic plans BitmapAnd `observed_window_start` with `(logical_source_id, calls)`.
  - Contexts come from `event_context` by `event_id` for the top 50 only.
  - With a match, see "Match filter" below.
- **outliers:**
  - The same scan of events for the range, grouped by (logical source, fingerprint) only, with the source's text columns joined after. Grouping by them too made generic plans sort every in-range event by four text keys.
  - A second scan of events for the history before the range, grouped into one array of samples per (logical source, fingerprint), hash-joined to the range's groups. The median and the median absolute deviation come from `percentile_cont` over each array. Joining to the range's groups first invited one index probe per fingerprint, which was far slower.
  - The worst window's start is looked up only for the at most 50 limited rows, by source and fingerprint (through `events_source_fingerprint_window` since migration 0014 dropped `events_fingerprint_window`). Carrying it through the aggregation (an ordered `array_agg`) sorted every in-range event, about 8s at 7d.
  - A 7d range reads 7 days of history, so it scans 15 partitions. That's most of the time at 7d. A 3h range reads a day of history, then up to 7 days for fingerprints short of samples, one index-only probe per short group (see "Outliers history index" below).
  - With a match, see "Match filter" below.
- **Adaptive lookback (outliers, task 20261005-020000-2):**
  - A fingerprint needs $8 (30) history samples to be scored. At 5-minute windows an hourly job has at most 24 in the day before a short range, so it was never scored. Now a (source, fingerprint) group with fewer than 30 samples in the default lookback (the range's length, 1 to 7 days) tops up with its newest older windows, up to 7 days before the range, until it has 30. A group with 30 in the default lookback is unchanged, and so is every 7d range.
  - On the perf data at 3h, 17,105 of the cluster's 17,975 groups in the range are short, and their older rows are about 955k, more than half of the source's rows in those 6 days. So the extension can't be a per-group index probe: one ordered probe per group through `events_fingerprint_window` (which has no source column, and visits every daily partition) took about 7.5s at 3h, and its plan cost (7M at 3h, 56M at 24h) added about 0.8s of JIT. So the `older` CTE read the 6 older days of the sources' rows once (bitmap scans on `(logical_source_id, calls)`), hash-semi-joined them to the short groups, and ranked each group's windows newest first. A `+ 0` on the join keys stopped the planner, which expects about one short group, from choosing a nested loop that reread the source per group. Scanning and joining those rows, with the rest of the report, is already about 1.65s at 3h, and sorting and ranking them takes about 1s more, so 2s couldn't hold, and the user set outliers' budget to 10s at every range.
  - A covering index on `events (logical_source_id, fingerprint_id, observed_window_start) INCLUDE (time, calls)` made the per-group probe about 2.7s at 3h, still not better. It wasn't added then. Task 20261004-231500-1 replaced this scan with per-group probes that stop early; see "Outliers history index" below.
  - More fingerprints are scored, so more are listed: 50 rows at 3h instead of 4, 22 for the primary instead of 1.
  - Before and after, interleaved in one run (load average about 12), cluster 13, median (custom / generic):

    | Report | Before | After |
    |---|---|---|
    | outliers 3h | 293ms / 353ms | 2.63s / 2.31s |
    | outliers 3h role=primary | 92ms / 226ms | 826ms / 1.03s |
    | outliers 3h match | 178ms / 228ms | 1.07s / 800ms |
    | outliers 24h | 482ms / 598ms | 3.58s / 3.33s |
    | outliers 24h role=primary | 172ms / 267ms | 1.23s / 1.20s |
    | outliers 24h match | 604ms / 616ms | 1.56s / 1.33s |
    | outliers 7d | 3.52s / 4.92s | 3.57s / 4.89s |
    | outliers 7d role=primary | 1.23s / 1.68s | 1.24s / 1.71s |
    | outliers 7d match | 4.82s / 5.18s | 6.11s / 4.92s |

    The 7d plans and results are unchanged, so the 7d differences are noise; before, one 7d match custom run took 9.1s.
- **JIT off (task 20261004-225300-1):**
  - Reports now run with `jit = off`, set transaction-locally next to the statement timeout in the UI's `ReportRunner` and in this suite. Postgres JIT-compiles a plan whose estimated cost passes `jit_above_cost` (100k), and inlines and optimizes it past 500k. The reports' plans cross those lines often (outliers at every range since the adaptive lookback, and generic plans, whose estimates run high, even at 3h), and the compile time was never paid back: these queries spend their time in scans, hashing and sorting, not in expression evaluation.
  - Back-to-back runs, JIT on (the suite as it was) then JIT off, at a load average of about 14–18. Medians, custom / generic:

    | Case | JIT on | JIT off |
    |---|---|---|
    | top_by_calls 3h | 34ms / 90ms | 34ms / 39ms |
    | top_by_calls 3h match | 54ms / 161ms | 51ms / 51ms |
    | top_by_calls 24h | 925ms / 532ms | 190ms / 285ms |
    | top_by_calls 24h match | 471ms / 494ms | 259ms / 351ms |
    | top_by_calls 7d | 2.48s / 3.74s | 2.24s / 3.82s |
    | top_by_calls 7d match | 3.99s / 3.91s | 3.58s / 3.86s |
    | top_by_total_time 24h | 308ms / 633ms | 193ms / 280ms |
    | top_by_total_time 7d | 2.30s / 3.35s | 2.11s / 3.03s |
    | top_by_total_time 7d match | 3.62s / 4.05s | 3.71s / 3.71s |
    | outliers 3h | 2.58s / 2.29s | 2.31s / 1.99s |
    | outliers 3h role=primary | 823ms / 782ms | 662ms / 572ms |
    | outliers 3h match | 1.13s / 1.28s | 813ms / 608ms |
    | outliers 24h | 3.68s / 3.34s | 3.10s / 2.78s |
    | outliers 24h role=primary | 1.52s / 1.67s | 958ms / 939ms |
    | outliers 24h match | 1.70s / 1.36s | 1.30s / 1.05s |
    | outliers 7d | 3.71s / 4.96s | 3.71s / 4.89s |
    | outliers 7d role=primary | 1.25s / 2.49s | 971ms / 1.33s |
    | outliers 7d match | 4.72s / 4.76s | 4.57s / 3.97s |

    JIT off was as fast or faster in nearly every case; the few slower ones are within run-to-run noise on this machine. The Results tables above predate this change, so they include JIT time.
- **Match filter (top_by_calls, top_by_total_time and outliers, task 20261005-020000-1):**
  - `context_events` reads the range's `event_context` rows once, through `event_context_source_window`, grouped by (controller_id, action_id, job_tag_id) with an `array_agg` of event ids. On cluster 13 at 7d that's about 2.5M rows but only 340 distinct contexts, so the regex runs 340 times instead of once per row. `matched_events` (materialized) unnests the matching contexts' event ids: 16 contexts and about 100k events at 7d. Before, the regex ran on every row, after joining every row to controllers, actions and job tags.
  - The range's aggregate flags each group with `bool_or` over a left join to `matched_events` (one hash, built once). Before, `bool_or(e.id in (select ...))` probed a hashed subplan for every in-range event.
  - The text match moved out of the aggregate's HAVING into `text_matched`, which probes `fingerprints` by id (`= any(array(...))`) for the groups without a matching context. At run time, the correlated lookup per group cost about the same. But the planner priced it per estimated group (about 100k), which pushed the custom plan's total cost past `jit_optimize_above_cost` (500k). That added 1.4–1.7s of JIT inlining and optimization at 7d. outliers' custom plan now costs about 487k, just under the line. Reports now run with JIT off (see "JIT off" above).
  - outliers no longer carries an `array_agg` of event ids through its aggregate, which made the planner sort every in-range event. It looks up the event ids for the at most 50 limited rows by source and fingerprint (through `events_source_fingerprint_window` since migration 0014), and it joins `sources` for the text columns last. When matching, its history scan is restricted to the matching (source, fingerprint) groups. top_by_calls and top_by_total_time keep the `array_agg`: their limited rows are the hottest fingerprints, so a second lookup costs more than carrying the arrays (about +0.4s at 7d without a match).
  - Results are unchanged. On the perf data, outliers returned the same rows as before the rewrite for context, job tag, text, no-match and NULL regexes. top_by_calls and top_by_total_time returned the same rows for the controller#action regex at 3h and 7d. `match_filter_test.go`, which covers each kind of match, passes.
  - Before and after, interleaved in one run, cluster 13, 7d, median (custom / generic):

    | Report | Before | After |
    |---|---|---|
    | top_by_calls match | 8.8–11.9s / 4.8–5.7s | 3.4–4.6s / 3.7–3.8s |
    | top_by_calls (no match) | 2.04–2.08s / 3.09–3.72s | 2.16–2.24s / 3.19–3.23s |
    | outliers (no match) | 3.99–4.06s / 5.79–5.99s | 3.36–3.44s / 4.73–4.79s |

    top_by_total_time has the same shape as top_by_calls. Before the rewrite, it took about 12.5s / 7.8s at 7d with a match on a busier machine. outliers 7d match took 7.45s / 8.57s in this task's red run, and 8.1s / 8.4s in the table this page had before.
- **replica_utilization:**
  - Reads only `event_context`, through `event_context_source_window` on `(logical_source_id, observed_window_start)`: a bitmap heap scan in custom plans, an index scan in generic plans. There's no join to events and no window function (migration 0011, see below).

### Per-fingerprint reports (cluster 13, hot fingerprint id 1 / typical fingerprint id 200)

| Report | Range | Custom (hot / typical) | Generic (hot / typical) | Scanned | Removed (generic) |
|---|---|---|---|---|---|
| fingerprint_timeseries | 3h | 1ms / 1ms | 1ms / 1ms | 1/1 | 30 |
| fingerprint_contexts | 3h | 2ms / 1ms | 1ms / 1ms | 1/1 | 60 |
| fingerprint_sources | 3h | 1ms / 1ms | 1ms / 0ms | 1/1 | 30 |
| fingerprint_all_sources | 3h | 1ms / 1ms | 1ms / 1ms | 1/1 | 30 |
| fingerprint_timeseries | 7d | 60ms / 2ms | 16ms / 2ms | 8/8 | 23 |
| fingerprint_contexts | 7d | 90ms / 18ms | 248ms / 9ms | 8/8 | 46 |
| fingerprint_sources | 7d | 65ms / 2ms | 14ms / 1ms | 8/8 | 23 |
| fingerprint_all_sources | 7d | 13ms / 2ms | 9ms / 1ms | 8/8 | 23 |
| fingerprint_timeseries | 21d | 348ms / 5ms | 324ms / 4ms | 21/21 | 10 |
| fingerprint_contexts | 21d | 517ms / 37ms | 1.06s / 57ms | 21/21 | 20 |
| fingerprint_sources | 21d | 317ms / 7ms | 320ms / 3ms | 21/21 | 10 |
| fingerprint_all_sources | 21d | 155ms / 6ms | 169ms / 2ms | 21/21 | 10 |

- **fingerprint_all_sources** (added later, task 20261003-170000-2; numbers from its own run) isn't limited to cluster 13: it sums every source's events. It reads only events and the source-0 `fingerprint_stats` row, with no join to `logical_sources`. With no source to filter on, it reads `events_source_fingerprint_window` with an index-only btree skip scan (Postgres 18) over every source in each partition; see "Dropping `events_fingerprint_window`" below.

- **Plan shape:** all of these read `events_source_fingerprint_window`, one index scan per source, mostly index-only, since its INCLUDE columns cover what they read. Before migration 0014 they used `events_fingerprint_window`, and the hot fingerprint's custom plans BitmapAnd it with `(logical_source_id, calls)`. `fingerprint_contexts` then reads `event_context` by `event_id`.
- **Role filter:** the `role=replica` variants of fingerprint_timeseries run at about half to two-thirds of the unfiltered time. For example, 21d hot is 209ms custom and 161ms generic.

### Findings

- **Pruning works for every report.** In both custom and generic plans, only the partitions overlapping the range are scanned, and the default partition never is. Custom plans prune at plan time. Generic plans prune at executor startup ("Subplans Removed"), and replica_utilization's generic plan also prunes per loop.
- **The role filter doesn't defeat index use.** With `($N::text is null or role = $N::text)`, the events access paths are the same with and without a role. The `role=` variants are faster because they touch fewer sources.
- **Every report is within budget at every measured preset.** The slowest is replica_utilization_by_controller_action at 21d with a generic plan, about 5.4s. That's within the 15s timeout, with roughly 3× headroom on this data set. See "Known limits".

## replica_utilization rewrite

**Red.** The first run with 7d cases had replica_utilization_by_controller_action's generic plan canceled by the 15s statement timeout. That's a real UI error page.

**Cause.** The report computed `ctx_total`, the sum of an event's context `c`, with a window function over every event_context row in the range, for all sources. Only then did it join that to the selected cluster's events. Custom plans could push the join into the window's partition (`event_id`) and probe by index. Generic plans couldn't: they seq-scanned and sorted every context row in the range. At 24h that took 2.6s, about 7M buffer hits; at 7d it ran past 15s.

**Fix** (both replica_utilization reports, same results):

1. Join events → sources → event_context first, and compute the window over that join. `ctx_total` still covers all of an event's context rows, before the controller/action or job-tag filter. It just no longer covers other sources' events. The existing replica_utilization tests pass unchanged.
2. Join `event_context` on `observed_window_start = e.observed_window_start` as well as `event_id`. Ingest (`internal/ingest/submit.go`) writes each context row with its event's window, so this adds no restriction in practice. It lets the generic plan's nested loop prune `event_context` to the event's own partition on every loop, instead of probing the `event_id` index of every partition in the range.

| replica_utilization_by_controller_action | Custom | Generic | Generic buffer hits |
|---|---|---|---|
| 3h, before | 210ms | 293ms | 0.5M |
| 3h, after | 78ms | 127ms | 0.14M |
| 24h, before | 752ms | 2.56s | 7.0M |
| 24h, after | 635ms | 716ms | 1.1M |
| 7d, before | not measured | **timed out (15s)** | n/a |
| 7d, step 1 only | 5.37s | 8.48s | 48.8M |
| 7d, after | 5.37s | 5.52s | 7.8M |

The job report moved the same way: at 24h generic it went from 1.17s to 448ms, and at 7d from 6.22s with step 1 only to 3.23s. The "before" rows come from runs before the seed was made deterministic, so they compare a statistically equivalent data set, not an identical one.

## replica_utilization at 21 days (migration 0011)

**Red.** With the rewrite above, cost still grew with the cluster's events in the range: each run joined them to event_context and sorted for `ctx_total`. 21d cases timed out replica_utilization_by_controller_action in both plans; by_job took 9.4s custom and 10.2s generic.

**Fix.** Migration 0011 adds `event_context.logical_source_id` and `event_context.attributed_time`: the event's source, and `events.time * c / sum(c)` over the event's contexts. Ingest writes both, the migration backfills existing rows, and it adds `event_context_source_window` on `(logical_source_id, observed_window_start)`. The reports now filter and sum `event_context` alone. Results are unchanged: the existing replica_utilization tests pass, the perf row counts match, and `internal/migrate` and `internal/ingest` tests check that the stored share equals the old window computation exactly.

| Median (custom / generic) | Before | After |
|---|---|---|
| controller_action 7d | 6.80s / 6.73s | 517ms / 1.88s |
| controller_action 21d | **timed out / timed out** | 2.09s / 5.42s |
| job 7d | 3.60s / 3.58s | 269ms / 548ms |
| job 21d | 9.36s / 10.25s | 967ms / 1.71s |

Alternatives measured on the same seed:

- **`events.context_total` only** (keep the join, drop the window): 21d custom 9.0s, and generic still timed out. Its backfill took 23 minutes.
- **The same columns without the index:** faster on this seed (21d 1.3s / 3.6s), because cluster 13 holds most of the rows and a seq scan wins. On a database with many clusters, the index keeps the cost proportional to the selected cluster, not to everything in the range. Building it took 5 seconds.

The backfill took about 7 minutes on 13M context rows. See `docs/database.md` for its locking.

## Index decision: `events (fingerprint_id, observed_window_start)`

**Decision: add it.** It's migration `0008_events_fingerprint_window.sql`, created on the partitioned parent, so every existing and future partition gets it. **Reversed by migration 0014**, which drops it: migration 0013's index serves the same reports as well (see "Dropping `events_fingerprint_window`" below). This section is the original decision's record.

**Why.** Without the index, `fingerprint_timeseries`, `fingerprint_contexts` and `fingerprint_sources` can only narrow by time and source. They read every heap page of the source's partitions in the range, so their cost grows with the size of the source, not with how many events the fingerprint has. A typical fingerprint costs as much as the hottest one.

Medians in the same run, with the index dropped inside a rolled-back transaction:

| Case | Plan | Without | With |
|---|---|---|---|
| fingerprint_timeseries 3h typical | custom / generic | 17ms / 11ms | 1ms / 1ms |
| fingerprint_timeseries 7d typical | custom / generic | 230ms / 321ms | 2ms / 2ms |
| fingerprint_contexts 7d typical | custom / generic | 257ms / 362ms | 18ms / 9ms |
| fingerprint_sources 7d typical | custom / generic | 235ms / 355ms | 2ms / 1ms |
| fingerprint_timeseries 21d typical | custom / generic | 842ms / 954ms | 5ms / 4ms |
| fingerprint_contexts 21d typical | custom / generic | 913ms / 998ms | 37ms / 57ms |
| fingerprint_sources 21d typical | custom / generic | 788ms / 896ms | 7ms / 3ms |
| fingerprint_timeseries 7d hot | custom / generic | 242ms / 364ms | 60ms / 16ms |
| fingerprint_timeseries 21d hot | custom / generic | 831ms / 993ms | 348ms / 324ms |
| fingerprint_contexts 21d hot | custom / generic | 634ms / 1.56s | 517ms / 1.06s |
| fingerprint_sources 21d hot | custom / generic | 820ms / 939ms | 317ms / 320ms |

**Effect:**
- **The typical fingerprint gains roughly 14–300×.** The hot fingerprint, which has an event in nearly every window, gains 1.2–23×.
- **Without the index, all cases would still fit the 2s budget at 10M events.** But they grow with table size. With the index, they grow with the fingerprint's own event count instead.

### Cost

- **Size:** 392 MB across 21 partitions, against an 891 MB heap. For comparison, the existing indexes are `observed_window_start` 93 MB, `observed_window_end` 93 MB, `(logical_source_id, calls)` 340 MB and `(logical_source_id, time)` 399 MB.
- **Ingest:** inserting one hour of windows for every stream takes, as a median of 5 rolled-back runs:

  | Run | Events inserted | With the index | Without |
  |---|---|---|---|
  | One | 20,469 | 399ms | 367ms |
  | Another | 20,424 | 499ms | 374ms |

  That's about 10–30% more insert time. It comes from one more btree insert per event.
- **Build time:** building the index (migration 0008's own statement) over the 10M seeded events took **3.0–3.1s** on this machine.

### Migration lock

- **Not concurrent:** the migration is a plain `CREATE INDEX` on the parent, not `CONCURRENTLY`. `CONCURRENTLY` isn't supported on partitioned tables. It would need an `ON ONLY` index plus a concurrent build and `ATTACH` per partition in a non-transactional goose migration.
- **What blocks:** for the whole build, it holds a SHARE lock on `rotten.events` and every partition, which blocks ingest inserts. Reads, and so reports, are unaffected.
- **How long:** at this size that's a few seconds. Scale it by the production events volume: the build is roughly linear in rows. Run it at a quiet time.
- **No data loss:** workers keep unsent batches in their outbox and retry, so ingest only stalls.

### Side effect on outliers

The `global_range_samples` CTE in `outliers.sql` used the new index. Task 20261004-221500-1 replaced it: outliers now builds its baseline from the events before the range. It used `events_fingerprint_window` only to find the worst window of each listed row, until migration 0014.

## Outliers history index (migration 0013)

Task 20261004-231500-1. **Decision: add `events_source_fingerprint_window`** on `events (logical_source_id, fingerprint_id, observed_window_start) INCLUDE (observed_window_end, calls, time)`, and read each short group's older windows through it.

**The query.** `older` is now a `LATERAL` per short group: an index-only scan of that (source, fingerprint), newest window first, `FETCH FIRST (30 - n) ROWS WITH TIES`. `WITH TIES` keeps the whole last window, exactly like the old `rank() <= 30 - n`, and the scan stops as soon as it has enough. At 3h, the 17,105 short groups (14,842 of which can reach 30) read about 295k index entries instead of 955k heap rows, with no sort or rank. It's still a Merge Append over the 7 older daily partitions per group, because `$4 - interval` can't prune at plan time.

**Measured alternatives** (3h, cluster 13, custom / generic):

| Option | 3h |
|---|---|
| Before: scan the sources' older rows, hash-semi-join, rank | 1.52s / 2.11s |
| Per-group probe through `events_fingerprint_window` (no source column) | about 7.5s |
| Rollup per (logical source, fingerprint, day) of window counts, then probes through `events_fingerprint_window` | 7.8s |
| Rollup, then probes through the new index without `observed_window_end` | 1.75s / 1.70s |
| The new index without `observed_window_end`, `WITH TIES`, no rollup | 1.35s / 1.23s |
| **The new index with `observed_window_end`, `WITH TIES` (index-only)** | **0.79s / 0.84s** |

The rollup (1.83M rows, 162 MB at 10M events; about 370ms to read on its own) only says how far back to go; the report still has to read the samples from events, so it can't beat the index on its own. It'd also need an upsert per event at ingest, contended across the hosts of one logical source, and it could drift from events: pg_partman's moves out of the default partition and partition drops don't decrement it. An index can't drift.

**Before and after**, interleaved in one session (load average 20–27), cluster 13, median (custom / generic). Results were identical (see below).

| Case | Before | After |
|---|---|---|
| outliers 3h | 1.52s / 2.11s | 726ms / 804ms |
| outliers 3h role=primary | 447ms / 540ms | 266ms / 277ms |
| outliers 3h match | 476ms / 729ms | 125ms / 143ms |
| outliers 24h | 2.27s / 3.35s | 1.21s / 1.31s |
| outliers 24h role=primary | 718ms / 925ms | 459ms / 494ms |
| outliers 24h match | 963ms / 1.10s | 625ms / 631ms |
| outliers 7d | 3.00s / 5.03s | 2.63s / 4.94s |
| outliers 7d role=primary | 805ms / 1.25s | 787ms / 1.29s |
| outliers 7d match | 3.40s / 3.89s | 3.58s / 4.22s |

7d has no short groups, so it doesn't change. In the suite, the red run on master took 2.10s / 1.82s at 3h (load average 30–69), and the final run 976ms / 898ms at 3h and 1.53s / 1.81s at 24h (load average about 15–20). 24h's budget stays at 10s.

**Same results.** On the perf data, the old and new SQL returned identical output at 3h (all roles, primary, a context match, a job match), 1h, 6h mid-data, and 24h, and, with a threshold of -1e12 so every scored group is listed, at 3h (15,712 rows), 24h replica and two matches. `outliers_history_test.go` checks the report against a Go implementation of its header on generated data: short and frequent groups, two replica hosts sharing windows (ties at the last window), windows exactly on the 7-day and lookback bounds, windows straddling the range's start, `calls = 0`, another cluster and the three kinds of match. It passes on both versions, and fails if `WITH TIES` becomes `ONLY` or the 7-day bound becomes exclusive.

### Cost

- **Size:** 846 MB as the seed's inserts grew it, 647 MB freshly built, against an 891 MB heap. Without `observed_window_end` it was 564 MB, but then each probe visits the heap.
- **Ingest:** one hour of windows for every stream (about 20k events), median of 5 rolled-back runs, without the index and with it: 490ms and 595ms in an interleaved harness (10 runs), 506ms and 562ms in the suite's final run. About 10–20% more insert time, from one more btree per event. (Another suite run measured 813ms and 712ms, so take single runs as noisy.)
- **Build time:** migration 0013's own statement took 6.5–7.8s over 10M events on this busy machine, about twice 0008's 3.4s, since the entries are wider.

### Migration lock

The same as 0008's: a plain `CREATE INDEX` on the partitioned parent, so a SHARE lock on `rotten.events` and every partition blocks ingest inserts for the build, while reads go on. `CONCURRENTLY` would need an `ON ONLY` index, a concurrent build and `ATTACH` per partition, in a non-transactional migration; `internal/migrate` runs SQL migrations in a transaction. It needs no grants. New partitions get the index.

### Effect on the per-fingerprint reports

The new index leads with the source, which the per-fingerprint reports filter on too, so with `events_fingerprint_window` dropped they're about as fast as with it (the suite's last table: e.g. fingerprint_timeseries 21d typical 3ms / 1ms with only the new index, 3ms / 1ms with both, 732ms / 1.21s with neither). Migration 0014 dropped `events_fingerprint_window`; see the next section.

## Dropping `events_fingerprint_window` (migration 0014)

Task 20261005-123457-1 (user decision, 2026-10-04: drop it if nothing regresses). Migration `0014_drop_events_fingerprint_window.sql` drops 0008's index, leaving 0013's `(logical_source_id, fingerprint_id, observed_window_start) INCLUDE (observed_window_end, calls, time)`.

### What read it

Every query that reads `rotten.events` by fingerprint is a report; `internal/` only inserts events, and the UI runs only the report SQL.

| Report | Filters by source? | Notes |
|---|---|---|
| fingerprint_timeseries, fingerprint_contexts, fingerprint_sources | yes, the cluster's (and role's) sources | one index scan per source |
| fingerprint_all_sources | **no** | every source; the risk, since the remaining index leads with the source |
| outliers (worst-window and event-id lookups for the listed rows) | yes, each row's source | |
| unparsed_summary | yes, the cluster's sources | starts from the unparsed fingerprints |

So the suite gained `unparsed_summary` cases, and `TestPerfManySources`, which adds a fleet of 400 more logical sources (see "Fleet" above) and a fingerprint on all of them (`fingerprint_all_sources … many`).

### Results

Each test's decision table, with 0013's index in both columns (median, custom / generic). Its "without" column is the schema with migration 0014, and its "with" column builds 0008's index again in the same rolled-back transaction.

`TestPerfReports` (main seed):

| Case | Without 0008's index | With it |
|---|---|---|
| fingerprint_timeseries 21d hot | 5ms / 5ms | 5ms / 4ms |
| fingerprint_timeseries 21d typical | 3ms / 1ms | 3ms / 1ms |
| fingerprint_contexts 21d hot | 387ms / 871ms | 484ms / 1.20s |
| fingerprint_contexts 21d typical | 42ms / 48ms | 39ms / 54ms |
| fingerprint_sources 21d hot | 4ms / 4ms | 6ms / 4ms |
| fingerprint_all_sources 21d hot | 6ms / 5ms | 7ms / 171ms |
| fingerprint_all_sources 21d typical | 2ms / 2ms | 3ms / 3ms |
| unparsed_summary 24h | 2ms / 1ms | 2ms / 2ms |
| unparsed_summary 7d | 3ms / 2ms | 4ms / 3ms |
| outliers 3h | 850ms / 833ms | 865ms / 1.19s |
| outliers 7d | 2.62s / 4.14s | 2.72s / 4.12s |

`TestPerfManySources` (with the fleet; fingerprint_all_sources only):

| Case | Without 0008's index | With it | With neither index |
|---|---|---|---|
| 3h hot / typical / many | 1ms / 1ms each | 1–2ms | 6–9ms |
| 7d hot | 6ms / 5ms | 7ms / 8ms | 115ms / 361ms |
| 7d typical | 5ms / 5ms | 2ms / 3ms | 95ms / 356ms |
| 7d many (400 sources) | 11ms / 10ms | 11ms / 18ms | 104ms / 339ms |
| 21d hot | 14ms / 13ms | 14ms / 171ms | 271ms / 996ms |
| 21d typical | 11ms / 10ms | 3ms / 3ms | 254ms / 1.08s |
| 21d many (400 sources) | 20ms / 29ms | 19ms / 53ms | 231ms / 938ms |

No report is more than 2× and 25ms slower without it. fingerprint_all_sources typical is a few milliseconds slower with the fleet (one index search per run of sources in each partition, rather than one); the other differences above a few milliseconds go both ways and change from run to run. The full tables are in the suite's output. The red run, on the schema before 0014 with the fleet in the main seed and the index dropped in the transaction instead, gave the same picture (e.g. fingerprint_all_sources 21d hot 17ms / 15ms without it, 27ms / 225ms with it). With the fleet, unparsed_summary 7d generic took about 2.1s with or without the index, another symptom of the skewed per-source estimate (see "Known limits"); it's 2–3ms on the main seed.

**fingerprint_all_sources needs Postgres 18's skip scan.** With no source to filter on, it reads `events_source_fingerprint_window` with an index-only skip scan: about 9–19 index searches per daily partition, though each partition holds 408 sources. Without any index on the fingerprint it takes 0.2–0.6s custom and 0.9–1.6s generic at 21d (hot, typical and many, over three runs), and Postgres 14–17, which have no skip scan, would be at best somewhat better than that: a full scan of the index or of the partitions. The rotten database runs only Postgres 18 (see [plan.md](plan.md)), so this is a constraint on its version, not a regression.

### Savings

- **Size:** `events_fingerprint_window` was 401 MB at 10.4M events, against a 926 MB heap and 872 MB for 0013's index.
- **Ingest:** one btree insert per event fewer. One hour of windows (about 20,400 events), median of 5 rolled-back runs, with it and without it: 634ms and 523ms, 482ms and 611ms, 562ms and 546ms, 579ms and 524ms, and 642ms and 754ms in five suite runs at load average 20–30. That's within run-to-run noise here; earlier interleaved measurements put one such index at about 10–20% of insert time.
- **Build time if it's ever restored:** 3.4–5.8s over 10–10.4M events (0013's: 5.8–7.4s).

### Migration lock

`DROP INDEX` on the partitioned parent drops the index on every partition, holding an `ACCESS EXCLUSIVE` lock on `rotten.events` and all its partitions. `DROP INDEX CONCURRENTLY` isn't supported on a partitioned index. Nothing is rebuilt, so the lock is held only briefly, but the drop first waits for running reports, and reports and inserts queue behind it. See [database.md](database.md).

### The decision check

The suite now requires 0013's index and checks 0008's in both directions: it fails if `events_fingerprint_window` exists while no report needs it, or if it's absent while one does (more than 2× and 25ms slower without it, or timing out only without it), and, when it exists, if a report is over budget without it. Both sides are measured in the same transaction, the schema's own state first and then the toggled one: comparing against the main run instead flagged `unparsed_summary 24h` once each way (2ms in one pass, 37–48ms in the other), since the timed inserts in between leave dead rows in the latest partition.

## Red notes

- **Original budgets passed from the start.** The first full run had 3h and 24h cases only, and passed every pruning and latency budget.
- **Index-decision red.** The test was extended to require `events_fingerprint_window` and to prove it pays off. It failed with "index events_fingerprint_window is missing" until migration 0008 was added.
- **7d red (review round 1).** Adding the 7d cases timed out replica_utilization_by_controller_action's generic plan, the "canceling statement due to statement timeout" error. It passed after the rewrite above.
- **Outliers red (task 20261004-221500-1).** Per-window scoring with a fixed 7-day history timed out at 3h (over 15s, as one index probe per fingerprint) and then took about 2.8s at 3h against the 2s budget. The history is now the range's length, at least a day. Later, the 7d generic plans timed out: the planner joined the range's groups twice, once matching on the source only. Scoring in one join and grouping by ids only fixed them.
- **Adaptive lookback (task 20261005-020000-2).** No version of the extension fit the old 2s budget at 3h: per-group probes took about 7.5s, and the scan in `outliers.sql` about 2.5s. The user raised outliers' budget to 10s at every range.
- **Match red (task 20261005-020000-1).** Tightening the outliers 7d match budget to 5s failed on time, not on an error: 7.45s custom and 8.57s generic. It passed after the match filter rewrite above.
- **Outliers history index (task 20261004-231500-1).** Tightening the outliers 3h budgets back to 2s failed on time: outliers 3h took 2.10s custom (and 1.82s generic) at load average 30–69. It passed after migration 0013 and the per-group `older` CTE.
- **Index drop (task 20261005-123457-1).** On the schema before migration 0014, the reversed decision check failed with "index events_fingerprint_window exists, but no report that reads events by fingerprint is more than 2× and 25ms slower without it". `TestEventsFingerprintWindowDropped` in `internal/migrate` failed while the index was there. Both passed after migration 0014.
- **21d red (task 20261003-190000-1).** Adding 21d replica_utilization cases timed out replica_utilization_by_controller_action in both plans. It passed after migration 0011.

## Known limits

- **replica_utilization reads every in-range context row of the cluster.** At 21d that's about 2.1s custom and 5.4s generic for controller_action on cluster 13, which holds most of the seeded rows. Cost is still linear in the cluster's context rows in the range, but it no longer joins events or sorts for a per-event total.
- **The other source-wide reports at 7d** take 0.5–2.1s with custom plans and up to 3.2s with generic plans, and up to 3.9s with a match. They grow linearly too.
- **outliers at 7d** takes about 3.5–5.2s custom and 4.8s generic without a match, and about 4.6s and 4.8s with one, because it also reads 7 days of history. It grows linearly in the range's events plus the history's.
- **outliers at 3h and 24h** takes about 0.8s and 1.3s, because fingerprints short of samples look up to 7 days back, one index probe per short group (see "Outliers history index"). 24h's budget stays at 10s.
- **Match reports' generic plans with hundreds of sources.** With the fleet's 400 sources seeded alongside (as in `TestPerfManySources`), the generic plans of top_by_calls, top_by_total_time and outliers with a match take about 2.1–2.9s at 3h (budget 2s) and time out at 24h and 7d, with or without `events_fingerprint_window`. The planner's per-source estimate drops (about 46 events where cluster 13 has about 35,000 in 3h), so it nested-loops the aggregate against the matched CTE. Custom plans are fine. Task 20261005-150200-1 tracks it. `TestPerfReports` seeds no fleet, so it doesn't catch this yet.
- **fingerprint_all_sources relies on btree skip scan**, so on Postgres 18 only (see "Dropping `events_fingerprint_window`").
- **JIT** is off for reports (see "JIT off" above), so plan costs crossing `jit_above_cost` or `jit_optimize_above_cost` as data grows no longer add compile time. Other queries on the rotten database keep the server's JIT settings.
- **Not covered:**
  - `fingerprint_stats` is seeded with `mean_time` rows only.
  - The source-wide reports other than replica_utilization aren't measured past 7d.
