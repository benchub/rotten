# Report query performance

This page covers how the report SQL in `reports/*.sql` performs against a realistically sized `rotten` database: about 10 million events across 21 daily partitions. It records latency, partition pruning, plan shapes, the `events (fingerprint_id, observed_window_start)` index decision, and the replica_utilization rewrite. The suite is `reports/perf_test.go` (task 20261001-103222-53).

## Reproducing

```sh
make test-perf                                  # about 10M events, around 9 minutes
make test-perf ROTTEN_PERF_EVENTS=500000        # quick smoke run
make test-perf PERF_TEST_ARGS='-run TestPerfReports'
```

- **What runs:** `make test-perf` runs `go test -tags perf ./reports` in the test image.
- **Not in the gate:** the `perf` build tag keeps the suite out of `make test`, `make test-unit` and `make test-all`. `make test` does run `go vet -tags perf ./reports`, so a compile break in the suite still fails the gate.
- **PG18 only:** the suite uses `internal/testdb` with a real Postgres in Docker, the rotten database image (`rotten-db-test:18`). The PG14–18 matrix applies to observed databases, not to the rotten database.
- **Output:** the results tables are printed with `-v`.

### What it does

1. **Seeds the data on the server side.** It uses `INSERT ... SELECT generate_series` over 6 parallel connections, one day at a time, then runs `VACUUM ANALYZE`.
   - **Determinism:** each day's insert runs `select setseed(...)` first, on the same connection, with a seed derived from the day's index. Contexts are derived from the event's natural key, not its id. The data ends at a fixed anchor, 2026-01-21 12:00 UTC. So every run seeds the same data: 10,039,225 events and 13,052,198 event_context rows. Every 3h range sits in one partition, and every 24h range spans two. Event ids and physical row order can still vary with scheduling.
   - **Sources:** 12 streams. Canvas clusters 13, 7 and 21 and bridge cluster 1 each have a primary with one host and a replica with two hosts. Cluster 13 carries 4× the weight of each other cluster. The reports query cluster 13.
   - **Windows:** 5 minutes long, spanning 21 days, so 21 populated daily partitions created by `public.create_partition_time`.
   - **Fingerprints:** pools of 15,000 for canvas and 5,000 for bridge. Fingerprints are picked with a skew (`pool*random()^2`), and calls are skewed too (`50000/idx`). On cluster 13 over 21 days, the hot fingerprint (id 1) has 16,549 events and the typical fingerprint (id 200) has 1,577.
   - **Outliers:** a slice of fingerprints is 10× slower in the last 2 hours, so `outliers` has something to find.
   - **event_context:** about 1.3 rows per event, 20% of them job contexts. Each row gets its event's `logical_source_id` and an equal share of its time as `attributed_time`, as ingest would write them (every context of a seeded event has the same `c`).
   - **fingerprint_stats:** `mean_time` rows per source and for source 0.

   Seeding takes 1m20s–2m15s.
2. **Runs every report the way the UI does.**
   - It connects as `rotten_ui` and uses `PREPARE` / `EXECUTE` with bound parameters.
   - Each run is in a read-only transaction with `statement_timeout = 15s`.
   - Each case runs under both `plan_cache_mode = force_custom_plan` and `force_generic_plan`. Rails reuses prepared statements, so after five executions Postgres may switch to a generic plan, where pruning happens at executor startup or per loop.
   - Each case gets one warmup, then the median of 5 runs (3 for the source-wide 24h and 7d cases).
   - A run canceled by the timeout is reported as a failure, and the suite carries on.
3. **Asserts pruning and latency.**
   - **Pruning:** `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` must scan only partitions that overlap the range, and at least one events or event_context partition (replica_utilization reads only event_context). A partition counts as scanned when its node has loops > 0. Generic plans report pruned partitions as "Subplans Removed".
   - **Latency budgets** (median):

     | Reports | Ranges | Budget |
     |---|---|---|
     | Every report | 3h | 2s |
     | The fingerprint reports | 3h, 7d, 21d | 2s |
     | top_by_calls, top_by_total_time, outliers, both replica_utilization reports | 24h and 7d | 15s |
     | Both replica_utilization reports | 21d | 15s |

     15s is the UI's `statement_timeout`, so it's the hard failure line: past it, the user gets an error page instead of a report. It's a ceiling, not a target. The 7d numbers below show how much headroom each report has.
4. **Decides on the index.**
   - The test fails if `events_fingerprint_window` is missing.
   - It logs index sizes and times a one-hour insert with and without the index.
   - It drops the index inside a transaction it rolls back, then reruns the fingerprint reports without it.
   - It requires the index to be at least 2× faster for the typical fingerprint at 7d and 21d.
   - Last, still inside that transaction, it rebuilds the index with migration 0008's own Up section and times the build.

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
| top_by_calls | 3h | 28ms | 57ms | 1/1 | 60 |
| top_by_calls role=replica | 3h | 17ms | 48ms | 1/1 | 60 |
| top_by_total_time | 3h | 26ms | 60ms | 1/1 | 60 |
| outliers | 3h | 259ms | 409ms | 2/2 | 149 |
| outliers role=primary | 3h | 89ms | 151ms | 2/2 | 149 |
| outliers match | 3h | 356ms | 453ms | 2/2 | 149 |
| replica_utilization_by_controller_action | 3h | 16ms | 28ms | 1/1 | 30 |
| replica_utilization_by_job | 3h | 6ms | 7ms | 1/1 | 30 |
| top_by_calls | 24h | 162ms | 331ms | 2/2 | 58 |
| top_by_calls role=replica | 24h | 111ms | 226ms | 2/2 | 58 |
| top_by_total_time | 24h | 158ms | 324ms | 2/2 | 58 |
| outliers | 24h | 437ms | 665ms | 3/3 | 145 |
| outliers role=primary | 24h | 140ms | 225ms | 3/3 | 145 |
| outliers match | 24h | 1.57s | 1.12s | 3/3 | 145 |
| replica_utilization_by_controller_action | 24h | 74ms | 207ms | 2/2 | 29 |
| replica_utilization_by_job | 24h | 36ms | 58ms | 2/2 | 29 |
| top_by_calls | 7d | 1.40s | 2.93s | 8/8 | 46 |
| top_by_calls role=replica | 7d | 940ms | 1.99s | 8/8 | 46 |
| top_by_total_time | 7d | 1.39s | 2.86s | 8/8 | 46 |
| outliers | 7d | 3.88s | 5.58s | 15/15 | 115 |
| outliers role=primary | 7d | 1.34s | 1.68s | 15/15 | 115 |
| outliers match | 7d | 8.09s | 8.37s | 15/15 | 115 |
| replica_utilization_by_controller_action | 7d | 517ms | 1.88s | 8/8 | 23 |
| replica_utilization_by_job | 7d | 269ms | 548ms | 8/8 | 23 |
| replica_utilization_by_controller_action | 21d | 2.09s | **5.42s** | 21/21 | 10 |
| replica_utilization_by_job | 21d | 967ms | 1.71s | 21/21 | 10 |

The replica_utilization rows are from task 20261003-190000-1's final run, after migration 0011. Since they read only `event_context`, their Scanned and Removed columns count event_context partitions. The outliers rows are from task 20261004-221500-1's final run, after per-window scoring. Their Scanned column counts events partitions, including the history before the range (the range's length, at least a day and at most 7 days).

Plan shapes:

- **top_by_calls and top_by_total_time:**
  - Custom plans use a bitmap scan on `(logical_source_id, calls)` up to 24h, then a seq scan of the 8 partitions at 7d.
  - Generic plans BitmapAnd `observed_window_start` with `(logical_source_id, calls)`.
  - Contexts come from `event_context` by `event_id` for the top 50 only.
- **outliers:**
  - The same scan of events for the range, grouped by (logical source, fingerprint) only, with the source's text columns joined after. Grouping by them too made generic plans sort every in-range event by four text keys.
  - A second scan of events for the history before the range, grouped into one array of samples per (logical source, fingerprint), hash-joined to the range's groups. The median and the median absolute deviation come from `percentile_cont` over each array. Joining to the range's groups first invited one index probe per fingerprint, which was far slower.
  - The worst window's start is looked up only for the at most 50 limited rows, through `events_fingerprint_window`. Carrying it through the aggregation (an ordered `array_agg`) sorted every in-range event, about 8s at 7d.
  - A 7d range reads 7 days of history, so it scans 15 partitions. That's most of the time at 7d. A 3h range reads a day of history.
  - The match cases spend most of their time in `matched_events`, as before per-window scoring: master's SQL took about 11s custom and 12s generic for 7d match in the same harness. Backlog task 20261005-020000-1 covers it.
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

- **fingerprint_all_sources** (added later, task 20261003-170000-2; numbers from its own run) isn't limited to cluster 13: it sums every source's events. It reads only `events_fingerprint_window` and the source-0 `fingerprint_stats` row, with no join to `logical_sources`, so it's cheaper than fingerprint_sources even for the hot fingerprint across all canvas clusters.

- **Plan shape:** all of these use `events_fingerprint_window`. The hot fingerprint's custom plans BitmapAnd it with `(logical_source_id, calls)`. `fingerprint_contexts` then reads `event_context` by `event_id`.
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

**Decision: add it.** It's migration `0008_events_fingerprint_window.sql`, created on the partitioned parent, so every existing and future partition gets it.

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

The `global_range_samples` CTE in `outliers.sql` used the new index. Task 20261004-221500-1 replaced it: outliers now builds its baseline from the events before the range. It uses `events_fingerprint_window` only to find the worst window of each listed row.

## Red notes

- **Original budgets passed from the start.** The first full run had 3h and 24h cases only, and passed every pruning and latency budget.
- **Index-decision red.** The test was extended to require `events_fingerprint_window` and to prove it pays off. It failed with "index events_fingerprint_window is missing" until migration 0008 was added.
- **7d red (review round 1).** Adding the 7d cases timed out replica_utilization_by_controller_action's generic plan, the "canceling statement due to statement timeout" error. It passed after the rewrite above.
- **Outliers red (task 20261004-221500-1).** Per-window scoring with a fixed 7-day history timed out at 3h (over 15s, as one index probe per fingerprint) and then took about 2.8s at 3h against the 2s budget. The history is now the range's length, at least a day. Later, the 7d generic plans timed out: the planner joined the range's groups twice, once matching on the source only. Scoring in one join and grouping by ids only fixed them.
- **21d red (task 20261003-190000-1).** Adding 21d replica_utilization cases timed out replica_utilization_by_controller_action in both plans. It passed after migration 0011.

## Known limits

- **replica_utilization reads every in-range context row of the cluster.** At 21d that's about 2.1s custom and 5.4s generic for controller_action on cluster 13, which holds most of the seeded rows. Cost is still linear in the cluster's context rows in the range, but it no longer joins events or sorts for a per-event total.
- **The other source-wide reports at 7d** take 0.6–1.4s with custom plans and up to 2.9s with generic plans. They grow linearly too.
- **outliers at 7d** takes about 3.9s custom and 5.6s generic without match, and about 8s with it, because it also reads 7 days of history. It grows linearly in the range's events plus the history's.
- **Not covered:**
  - `fingerprint_stats` is seeded with `mean_time` rows only.
  - The source-wide reports other than replica_utilization aren't measured past 7d.
