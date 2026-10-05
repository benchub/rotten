# Per-context attribution by sampling pg_stat_activity.

Date: October 4, 2026. Task: 20261004-150000-1. Revised the same day after
two review rounds (see [Changes after review](#changes-after-review)).

## Status.

Proposed. This is a design only. Nothing here gets built until the user signs
off. The questions that need sign-off are in
[Open questions for sign-off](#open-questions-for-sign-off), and the plain
trade-off is in [Is this worth building?](#is-this-worth-building).

## The problem.

`pg_stat_statements` (pgss) keeps one text per entry, keyed by
`(userid, dbid, toplevel, queryid)`: the first text it saw. Comments don't
change the `queryid`, so every call of an entry shares that one text. The
worker reads the controller, action and job tag out of it and credits all of
the entry's calls in a window to that context (`buildHarvestBatchFromRows`
in `internal/worker/worker.go`). The server then splits the entry's time the
same way (`event_context.attributed_time`).

If `UsersController#show` runs `select ... from users where id = $1` first,
and then a nightly job runs the same statement a million times, the UI says
`users#show` made a million and one calls and spent all their time. The
error can be any size, and it's invisible apart from a caveat.

## What sampling can and can't estimate.

A tick of `pg_stat_activity` only sees a statement while it runs, so a call
is caught with a probability that grows with its duration. That rules out
call counts. On Postgres 18.4 we ran two contexts through one pgss entry
(`select pg_sleep($1)` with different comments), each at 10 calls/s. Calls
in context *fast* took about 5 ms, and calls in *slow* about 100 ms. We
sampled with random (Poisson) ticks for 40 seconds:

| | fast | slow |
| --- | ---: | ---: |
| True calls | 440 (49.8%) | 444 |
| Distinct executions caught | 55 (11.7%) | 414 |
| Ticks that found it running | 57 (3.8%) | 1,449 |
| True exec time, from pgss | about 3.7% | about 96.3% |

The execution count is badly wrong as a call estimate. The tick count tracks
**time**: each tick that finds a context running stands for one tick
interval of that context being active. That's how a sampling profiler works.
Weighting caught calls by their duration doesn't rescue call counts either:
most calls end before the next tick and leave no trace.

So the most sampling can claim is this:

- **An activity share.** The fraction of the window's sampled moments in
  which each context was running a statement of the entry. We apply that
  share to pgss's exact time total, under an assumption, stated in the UI,
  that the **steady mix** of contexts during the window matches the mix of
  calls that completed in it (see
  [Estimating each context's share](#estimating-each-contexts-share)).
- **Not calls.** Per-context calls stay on first-text attribution, with
  today's caveat.
- **Not error bars.** One tick sees every backend at once, so concurrent
  executions aren't independent observations. We show how much data a
  share rests on, not a confidence interval (see
  [Statistical error](#statistical-error)).
- **A sighting, at the least.** Even when there's too little data for a
  share, one readable tick proves that a context ran the statement. That's
  enough to warn that a first-text count is mixed.

## Sampling pg_stat_activity.

`pg_stat_activity` shows each backend's current statement. Since Postgres 14
it has a `query_id` column, which is the same `queryid` pgss uses. At each
tick, the worker runs:

```sql
select clock_timestamp(), pg_backend_pid(), pg_postmaster_start_time(),
       pid, datid, query_id, query_start, octet_length(query), query
from pg_stat_activity
where state = 'active'
  and backend_type = 'client backend'
  and query_id is not null and query_id <> 0
  and pid <> pg_backend_pid()
```

- **Only running statements.** An idle session (`state = 'idle'` or
  `idle in transaction`) still shows its last statement, but that's idle
  time. Counting it would credit the pause after a statement to that
  statement, and would count one finished statement again at every tick.
- **Parallel workers** show their leader's query. `backend_type =
  'client backend'` leaves them out, so a parallel query counts once, as
  pgss does.
- **All databases.** pgss covers the whole cluster, so the sampler doesn't
  filter on `datid`.
- **Server-side clock and byte length.** `clock_timestamp()` puts ticks on
  pgss's clock. `octet_length(query)` is in server-encoding bytes, which is
  what truncation works in.
- **Identity columns.** `pg_backend_pid()` and `pg_postmaster_start_time()`
  bind each tick to one server (see [Node identity](#node-identity)).

### The sampling key, and SET ROLE.

A tick row belongs to the **group** of pgss entries with its
`(dbid, queryid)` and `toplevel = true`, not to one entry, because
`usesysid` doesn't reliably name the entry. `pg_stat_activity` reports the
**session** user, but pgss keys on the **current** user. We checked this on
14, 17 and 18: after `SET ROLE app_owner`, activity showed `app_login`, and
the pgss entry was `app_owner`'s. `SECURITY DEFINER` functions and
`SET SESSION AUTHORIZATION` do the same.

So a group's ticks can't be divided between its users' entries. The design
uses a group's ticks only when the group is **complete**:

- every entry in the current pgss read with that `(dbid, queryid)` and
  `toplevel = true` was picked for this harvest (in the top N, with text);
  and
- they all went to the same fingerprint.

If any member was left out, or the members fingerprint differently, the
group's ticks can't be matched to the time they describe, and the whole
group falls back to first text. Usually a group has one member, or a few
that are all busy, so this is rare. The cases where it bites (one busy user
and one quiet one, with only the busy one in the top N) are the ones where
guessing would be wrong.

Entries with `toplevel = false` (from `pg_stat_statements.track = all`)
never match: `pg_stat_activity` shows only the top-level statement.

### Multi-statement messages.

A simple-protocol message can hold several statements: `select 1; select 2`.
On 14, 17 and 18, all its statements share one `query_start`, `query` holds
the whole message, and `query_id` follows the statement running at the
moment. So the key is right, but the comment in the text may belong to
another statement.

The rule is strict:

1. If the text has a `;` followed by more SQL, the worker splits it with
   pg_query (`SplitWithScanner`) and reads the context from each statement.
2. The tick gets a context only if **every** statement gives the identical
   `(controller, action, job_tag)`, including all empty. A tagged statement
   next to an untagged one is **unknown**.
3. A multi-statement message that may be truncated is **unknown**, because a
   cut-off statement could carry a different comment.

### compute_query_id.

`query_id` is set only when `compute_query_id` is `on`, or `auto` with a
module that asks for it. pg_stat_statements asks for it when it's in
`shared_preload_libraries`, and `auto` is the default. So wherever pgss
works, `pg_stat_activity.query_id` is filled in too. We checked 14.24 and
18.4. Both default to `auto` and report `query_id` for the simple protocol,
the extended protocol, and named prepared statements re-run with custom and
with generic plans.

`query_id` is NULL or 0 in these cases, and the query skips them:

- a session that hasn't run anything yet;
- a statement that failed to parse;
- a session that ran `set compute_query_id = off` (NULL on 14 and 18). pgss
  doesn't count those calls either.

For **utility statements** (`SET`, `VACUUM`, `BEGIN`, `COMMIT`), 14 and 18
reported a `query_id` matching the pgss entry, with
`pg_stat_statements.track_utility` at its default of `on`. With
`track_utility = off`, pgss has no entry, so those ticks have no group and
are dropped.

### Privilege.

The observer already has `pg_read_all_stats` (`schema/observer.sql`). Without
it, other roles' rows show `<insufficient privilege>` and a NULL `query_id`.
`pg_postmaster_start_time()` needs no grant (checked on 14 and 18 as a plain
login role). No new grant is needed.

## Estimating each context's share.

All of this is per window and per complete group *G*.

- A tick row in *G* is one **hit**, with a context (possibly the empty
  context) or **unknown**.
- Hits are grouped into **executions** by `(pid, query_start)`.
- *h_c* is the hits with context *c*, *h_?* the unknown hits, and *h* the
  total.
- *T_G* is the sum of `metrics.total_time` over *G*'s entries. That's the
  same total the aggregate reports today: **planning plus execution**
  (`internal/pgss/reader.go:222`, `internal/worker/topn.go:25`).

If *G* qualifies (below), context *c* gets *T_G* × *h_c* / *h*, and
*T_G* × *h_?* / *h* is kept as **unattributed**. The parts add up to *T_G*
exactly. Otherwise *G* falls back to first text: each entry's whole time goes
to the context in its first text.

**Planning time** is split by the same share. The `active` state covers
parse, plan and execute, so the tick share already includes planning. We
assume planning splits like the rest, which is true when contexts share a
plan shape. Splitting `total_time` keeps the rows adding up to the
aggregate's total, so the UI never shows two different totals.

**What the share describes.** Ticks measure activity during the window. The
pgss delta counts calls that **finished** in the window. These match when the
mix of contexts is steady and statements are short next to the window. They
don't match when long statements span a harvest. A 4-minute report that's
still running at harvest has many ticks in this window but no time in this
delta, while short calls from another context fill the delta. So *G*
qualifies only if:

- it has at least **10 ticks with a hit** and at least **10 executions**;
- **boundary executions** hold at most 10% of its hits. A boundary execution
  was already running at the window's start, or is still running at the
  harvest. The worker finds these cheaply. Right after each harvest's pgss
  read, it takes one extra tick, which isn't counted as a hit. That tick's
  `(pid, query_start)` pairs are this window's still-running executions and
  the next window's carried-in ones.
- none of its members was reset or recreated in the window (see
  [Sample lifecycle](#sample-lifecycle)).

These rules exist to keep the estimate honest, not precise. The UI calls the
result an **activity share**, and says it assumes a steady mix.

### Unknown ticks.

A tick is unknown when its text can't be read with confidence. That happens
when it may be truncated and has no readable match, or when it's a
multi-statement message that fails the rule above. Unknown ticks **stay in
the denominator**, and their share becomes unattributed time. They're never
spread across the readable contexts, because what makes a text unreadable
can depend on the context. A context with a long comment is the one
truncation cuts.

### No pooling across windows.

Each window uses only its own ticks. Groups that don't qualify fall back.
Pooled samples would be counted in several windows when the UI sums them.

## How the estimates are stored.

Today the worker sends each fingerprint's contexts as `QueryContext` rows
(calls by first text). The server writes `event_context` with `c` = calls
and `attributed_time` = time × `c` / Σ `c`. That stays exactly as it is.

The design comes in two stages, so the cheap part can ship alone:

- **Stage 1, sightings.** For each aggregate, the worker sends how many ticks
  and executions sampling saw, and up to 3 readable contexts that were seen
  running it but aren't among its first-text contexts. Those are the
  contexts that ran the statement but got no credit. Sightings need only
  node identity and complete groups. They don't need the share rules,
  because one readable tick proves a context ran. The server stores them in
  `event_context_sighting`.
- **Stage 2, time split.** For aggregates with at least one qualifying
  group, the worker sends a **time attribution**: rows that split
  `metrics.total_time` and add up to it. Each row is one of four kinds:
  - **sampled:** a context and its estimated time, with its hits and
    executions;
  - **first text:** time from groups that didn't qualify, credited to their
    first-text context;
  - **unattributed:** time from unknown ticks;
  - **other:** small rows folded together by the cap.

  The server stores these in `event_time_attribution`. Time reports read it
  when an event has rows there, and otherwise read
  `event_context.attributed_time` as they do today. Aggregates where no group
  qualified send no time attribution, since `attributed_time` already says
  the same thing.

Call reports don't change in either stage.

## Bias against short queries.

A tick catches a statement in proportion to how long it runs. That's right
for an activity share, and the reason sampling can't count calls. What's
left is coverage. A group's expected hits in a window are about its active
time divided by the mean tick interval:

> expected hits ≈ active time / `ContextSampleInterval`

| Entry | Calls/s | Mean | Time per 5-min window | Hits at 1 s | Hits at 250 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Hot lookup | 200 | 0.5 ms | 30 s | 30 | 120 |
| Busy index scan | 20 | 5 ms | 30 s | 30 | 120 |
| Report query | 0.1 | 2 s | 60 s | 60 (30 executions) | 240 (30 executions) |
| Rare lookup | 1 | 1 ms | 0.3 s | 0.3 | 1.2 |

Entries get coverage in proportion to the time they cost the server. Cheap
or rare entries get first text, and since they cost little time, their
error moves little time. With the 10-second dev window, almost nothing
reaches 10 hits, so the dev stack mostly shows sightings.

## Sampling rate and cost.

`ContextSampleInterval` is the **mean** time between ticks: 1,000 ms by
default, and allowed from 100 to 60,000.

**Ticks are Poisson**: each gap is drawn from an exponential distribution
with that mean. A fixed period can lock onto a periodic workload. With a job
that runs every second for 20 ms, 1-second ticks see it either every time or
never, depending on phase. Poisson ticks see each state in proportion to the
time it lasts, whatever the workload's rhythm (the PASTA property: Poisson
arrivals see time averages). Uniform jitter around a fixed period mostly
breaks the lock too. Poisson costs the same and comes with the guarantee.

**Ticks share the harvest connection, and pauses are gaps.** Ticks run on
the observed-DB connection between harvests, so ticks and pgss reads come
from one server, with no second connection to pin. The cost is that ticks
stop while a harvest runs. We don't claim that's harmless. Anything that
happens mostly during harvests, including load the harvest itself causes, is
under-seen. So:

- the sampler records the time it wasn't sampling (harvest time, plus any
  failed tick) as a **gap**;
- if gaps are more than 10% of a window, the window's shares aren't used
  (sightings still are);
- otherwise the share describes the covered time, and the UI's wording
  ("sampled moments") doesn't imply more.

A typical harvest takes well under a second of a 5-minute window, about
0.3%.

**Per-tick deadlines.** The observed-DB harvest runs its queries with no
explicit transaction, so a tick can safely use its own. Each tick is sent as
one pgx batch, `begin; set local statement_timeout = '250ms'; select ...;
commit`, in a single round trip. A stalled `pg_stat_activity` read is
cancelled by the server after 250 ms, the connection stays usable, and the
tick counts as a gap. A Go deadline of 1 s backs that up against a dead
network. pgx v5's default cancel handler closes the connection when a
deadline fires. The worker then reconnects as it does today, the node
identity changes, and the window falls back. A tick never starts when the
next harvest is due within 250 ms, so a harvest waits at most one tick's
deadline. Its own transaction also gives each tick a fresh activity
snapshot.

We measured the query in Docker on an Apple-silicon laptop:

| Server | Sessions | Time per sample |
| --- | ---: | ---: |
| 14.24, `max_connections = 100` | 96 | about 1 ms |
| 18.4, `max_connections = 1000` | 909 | about 5–15 ms |

The time is mostly Postgres copying every backend's status slot, so it grows
with sessions and barely depends on the filter. At a 1-second mean, that's
under 1.5% of one core with about 900 connections, and about 0.1% on a
typical server. The worker caches each execution's context by
`(pid, query_start)`, so a long statement's text is parsed once. Memory is
a counter per (group, context) and the set of executions per group, a few
megabytes at most.

## Truncation by track_activity_query_size.

`pg_stat_activity.query` holds at most `track_activity_query_size − 1`
bytes, and the default is 1 kB. We checked this: a 2,636-character statement
showed as 1,023 bytes on 14 and 18, and its trailing comment was gone.
Postgres won't cut a multibyte character in half, and drops it whole. With
`é` at the cut, the text was 1,022 bytes on 14 and 18.

The rules:

- A text is **possibly truncated** when `octet_length(query)` is at least
  `track_activity_query_size − 4`. Four bytes is the longest character in
  any server encoding, so this catches every cut. A text that wasn't cut is
  sometimes treated as cut, which is harmless. The worker reads the setting
  when it connects.
- For a possibly truncated text, the patterns only run up to the last
  complete `*/`. A match there counts. No match makes the tick **unknown**,
  not the empty context.
- A possibly truncated multi-statement text is always unknown.
- Unknown time is reported as unattributed. A log line, once per process,
  gives the unknown share and suggests raising `track_activity_query_size`.

Raising the setting needs a restart. It costs `max_connections` × the size
in shared memory: for example, 16 MB for 1,000 connections × 16 kB.

## Postgres 18 and leading comments.

On 18, pgss drops a leading comment from the text it keeps. We checked this
on 18.4: `/*controller:users,action:show*/ select pg_sleep(3)
/*job_tag:trailing*/` was kept as `select pg_sleep($1) /*job_tag:trailing*/`.
`pg_stat_activity.query` keeps the whole text as sent. 14 kept both
comments in both places.

| Where the context is read | Leading comment, 18 | Trailing comment, any version |
| --- | --- | --- |
| Sightings and sampled time | Yes. A leading comment can't be cut by truncation. | Yes, if the statement fits in `track_activity_query_size`. |
| First-text calls, and time for groups that fall back | No | Yes |

Sampling brings leading comments back on 18 for sightings and the time split.
But calls, and most groups in practice, still come from the pgss text. So
the advice doesn't change: on 18, **append** comments, and raise
`track_activity_query_size` if statements are long.

The Postgres 18 no-contexts warning (`context_warning.go`) should learn about
sightings. If ticks see contexts that pgss texts don't, it should say that
leading comments are dropped from pgss but sampling sees them.

## Replicas.

Each replica has its own `pg_stat_activity` and pgss, and runs its own
worker. Sampling only reads, so it works on a hot standby. If pgss works on
the replica, `query_id` does too. The replica utilization reports compare
context time between primary and replica, so they gain the most.

### Node identity.

Ticks and the pgss read must come from the same server. `SanityCheck` doesn't
ensure that: two replicas both pass `select pg_is_in_recovery()`, and a
pooler or load balancer can move a connection between servers.

- Ticks run on the harvest connection, so there's no second connection to
  land somewhere else.
- Every tick and every harvest read `pg_backend_pid()` and
  `pg_postmaster_start_time()`. A physical replica shares its primary's
  `system_identifier`, so that isn't enough on its own. A backend pid
  together with a postmaster start time to the microsecond pins one process
  on one server.
- A window's ticks are used only if every tick and the harvest report the
  same pair. A reconnect, a restart, or a pooler handing over a different
  server connection changes the pair. Then the window's ticks are dropped,
  and the worker logs the cause once per process.
- The docs will say that `ObservedDBConn` must reach the server directly, or
  through a session-mode pooler. With a transaction-mode pooler, the pair
  keeps changing, and sampling quietly does nothing.

## Sample lifecycle.

Ticks live only in memory, for one window, and are cleared at each harvest.
The rules lean toward falling back:

| Case | What happens |
| --- | --- |
| Baseline harvest (no snapshot, stale snapshot, or a failed save last time) | All ticks dropped. Nothing is sent. |
| Node identity changed during the window | All ticks dropped. |
| Global reset: `pg_stat_statements_info.stats_reset` moved | No shares this window; every group falls back. Sightings are kept. |
| 17 and later: any member of a group has `stats_since` in the window | The whole group falls back. |
| 14 through 16: any member of a group is in `pgss.Recreated` (a counter went down, or `dealloc` moved, which marks every key) | The whole group falls back. |
| The group isn't complete (a member not picked, or split fingerprints) | The whole group falls back, and no sightings. |
| A tick after the harvest's pgss read (by `clock_timestamp()`) | Kept for the next window. |

A member reset partway through a window has a delta covering only part of
the window, while its group's ticks cover all of it. Rather than trim ticks
per member, which can't be done when the group has several users, the group
falls back. On 14 through 16, a server that evicts entries at most harvests
marks every key recreated, so shares never qualify there. The fix is the one
already documented: raise `pg_stat_statements.max`. The worker logs that
once. Sightings still work.

`TextCache` and `Retain` are independent: ticks aren't keyed by text, and the
first-text fallback uses the cache as it does now.

## Harvest size limits.

`internal/harvestlimits` sets `MaxHarvestContexts = 2000`, assuming one
first-text context per picked delta. The server rejects larger batches with
`InvalidArgument`, and the outbox sender drops those, metrics and all. So
sampling never adds `QueryContext` rows. It uses new fields with their own
limits:

- **Per aggregate, sightings:** at most 3 contexts, plus a flag saying that
  more were seen.
- **Per aggregate, time attribution:** at most **8 rows in total, across all
  kinds**. The unattributed row, if any, is always kept. The 6 largest
  named rows by time, sampled or first text, are kept. Everything else is
  folded into one "other" row, which carries only a time. An aggregate with
  no qualifying group sends no time attribution at all.
- **An aggregate sends one or the other.** With time attribution, its
  sampled rows already name the contexts, so it sends no sightings.
- **Per harvest:** at most 6,000 sighting and time-attribution rows
  together (`MaxHarvestSampledRows`). If a harvest would go over, the worker
  drops the time attribution from the aggregates with the least total time
  until it fits. Those fall back to `attributed_time`, and they keep their
  counts but no sighting names. The 3-sighting cap alone keeps sightings
  within 6,000, since a harvest has at most 2,000 aggregates.
- **Bytes:** a row is at most 3 × 512 bytes of strings plus about 40 bytes.
  6,000 rows add at most about 9 MiB. The existing worst case (2,000
  normalized texts of 8 KiB each, plus 2,000 contexts) is about 19 MiB.
  Together that's about 28 MiB, under the 32 MiB `MaxIngestMessageBytes`.
  The task checks this with an encoded worst-case batch.
- **One set of constants**, in `internal/harvestlimits`, which the worker
  already imports (`merge.go`). The worker trims, and the server validates,
  so a worker never sends what its own validator would reject.
- **Old servers.** An old server's protobuf decoder skips unknown fields,
  and its validator never sees them. The batch is accepted, and only the
  sampling data is lost. Deploy the server first. A new server with an old
  worker gets no fields, and the reports work as today.

## Statistical error.

The earlier drafts gave each share a 95% interval from a variance that
treated executions as independent. Review showed that's wrong. One tick
observes every backend at once, so concurrent executions move together. In
a simulation with 100 concurrent A executions followed by 100 concurrent B
executions, the intervals covered the truth in only 20% of trials, and 54%
had zero width. A correct interval would have to treat the tick, or a run
of correlated ticks, as the unit, and then be validated. That's a lot of
machinery for a number we'd still have to caveat.

So the design **shows no confidence intervals**. Instead:

- **Minimum samples.** A group needs at least 10 ticks with a hit and 10
  executions before its share is used. Otherwise it falls back.
- **Low data.** A sampled row built from any group with fewer than 30 ticks
  with a hit is flagged "low data".
- **The counts are shown.** Every sampled row shows its ticks and
  executions, so a reader can see how much it rests on.
- **A rough guide**, for the docs only. With *n* independent ticks, a 50%
  share is good to about ±18 points at 30 ticks, ±10 at 100, and ±5 at
  400. Bursty, concurrent workloads do worse than that, because their ticks
  aren't independent. The guide is a best case, not a bound.

The thresholds are set from the validation runs, including the
concurrent-burst case. If a low-data rule can't be made reliable, the UI
shows sampled shares only above some tick count, or only sightings.

## Showing estimates honestly in the UI.

- **Calls don't change.** Per-context call counts stay first text, with
  today's caveat.
- **Sightings** (stage 1) appear under a fingerprint's context table:
  "Sampling also saw this statement running from: `jobs#nightly`,
  `reports#index`. The first-text counts above don't include them." No
  numbers are attached.
- **Activity share** (stage 2) is a separate time view. A sampled row shows
  `≈`, its share and its tick count, for example `≈ 62% of time (140
  ticks)`. Rows are labelled "first text", "unattributed" or "other" as
  appropriate. Low-data rows are dimmed and say "low data".
- The caveat on the time view says that this is the share of sampled
  moments in which each context was running the statement, applied to the
  total, and that it assumes a steady mix during the window. It also says
  it's unreliable for statements that run longer than the window.
- No interval is shown, and no sampled number appears without its tick
  count.

## Fallback when there are no samples.

Every case without usable ticks keeps today's first-text attribution, for
calls and time:

- sampling is off, or the window's ticks were dropped (node change,
  baseline);
- the window had over 10% gaps, or a global reset;
- the entry is nested (`toplevel = false`);
- the group is incomplete, had a member reset or recreated, had fewer than
  10 ticks or executions, or had over 10% of its hits from boundary
  executions;
- the aggregate's time attribution was trimmed by the per-harvest cap.

A worker without the new config key behaves exactly as it does now. So does
an old worker with a new server, and a new worker with an old server.

## Config changes.

One new optional key, which makes this **opt-in**:

| Key | Required | Meaning |
| --- | --- | --- |
| `ContextSampleInterval` | no, default off | Mean milliseconds between `pg_stat_activity` samples (Poisson), from 100 to 60,000. Absent or 0 turns sampling off. 1000 is a good start. |

Nothing else changes: no new connection, grant or pattern. The thresholds,
caps, deadlines and truncation margin are constants. One documented
requirement comes with the key: `ObservedDBConn` must reach the server
directly, or through a session-mode pooler. The dev stack turns sampling on
at 250 ms in `dev/worker.json` and `dev/worker-replica.json`. `conf` shows
the key, and `docs/worker.md` gets a "Context sampling" subsection.

## Proto changes.

These are additive, so `buf breaking` passes. Stage 1 adds field 6, and
stage 2 adds field 7:

```proto
message FingerprintAggregate {
  // fields 1-5 unchanged; contexts (3) stays first-text calls
  ContextSampling sampling = 6;                   // stage 1
  repeated TimeAttribution time_attribution = 7;  // stage 2
}

message ContextSampling {
  uint32 ticks = 1;              // ticks that saw this fingerprint running
  uint32 executions = 2;
  // Up to 3 readable contexts seen running that aren't among the
  // first-text contexts, most ticks first. Empty when time_attribution
  // is sent.
  repeated SeenContext other_contexts = 3;
  bool more_other_contexts = 4;
}

message SeenContext {
  string controller = 1;
  string action = 2;
  string job_tag = 3;
}

message TimeAttribution {
  enum Basis {
    BASIS_UNSPECIFIED = 0;
    BASIS_SAMPLED = 1;
    BASIS_FIRST_TEXT = 2;
    BASIS_UNATTRIBUTED = 3;
    BASIS_OTHER = 4;
  }
  Basis basis = 1;
  string controller = 2;   // empty for UNATTRIBUTED and OTHER
  string action = 3;
  string job_tag = 4;
  double time = 5;         // ms, a part of metrics.total_time
  uint32 ticks = 6;        // SAMPLED only
  uint32 executions = 7;   // SAMPLED only
  bool low_data = 8;       // SAMPLED only
}
```

`internal/harvestlimits` validates:

- the per-aggregate and per-harvest caps;
- a known basis, and strings within `MaxContextStringBytes`;
- one row per (basis, context), and at most one unattributed and one other
  row;
- finite, non-negative times adding up to `metrics.total_time`, within 1e-6
  relative.

Migrations add `event_context_sighting` (stage 1) and
`event_time_attribution` (stage 2). Both are partitioned by
`observed_window_start` with pg_partman like `event_context`, with
`rotten_ingest` insert and `rotten_ui` select grants.

## Validation plan.

These run against real Postgres (`internal/testdb`, on 14 and 18), with known
ground truth. The truth comes from a control run in which each context's
statement is its own pgss entry, so pgss reports its time exactly. The
results are recorded in this doc, and they set the thresholds.

1. **Unequal latencies.** Two contexts at equal call rates, about 1 ms and
   100 ms. The sampled shares track the time shares. The execution-count
   split is recorded to show it's biased for calls.
2. **Periodic arrivals.** One context fires every 1,000 ms for 20 ms. With
   Poisson ticks, its share tracks the truth. A fixed-period run is recorded
   to show the phase lock.
3. **Concurrent bursts.** 100 concurrent A executions, then 100 concurrent
   B. Record how far the shares stray, and confirm the low-data flag fires
   or the group falls back.
4. **Long statements across windows.** A 4-minute A statement spans a
   harvest while short B calls run. The group falls back under the
   boundary rule rather than crediting A with B's time.
5. **Planning time.** With `pg_stat_statements.track_planning = on` and
   nonzero planning time, the rows add up to `total_time`, planning
   included.
6. **Context-dependent truncation.** One context's trailing comment pushes
   its text past `track_activity_query_size`. Its time shows up as
   unattributed, and the other context's share doesn't grow.
7. **Harvest pauses.** Simulated slow harvests push gaps over 10%, and that
   window's shares aren't used.

## Alternatives considered.

- **Sampled statement logging.** `log_min_duration_sample = 0` with
  `log_statement_sample_rate` logs a random fraction of statements,
  regardless of duration. That's the duration-independent call sample that
  `pg_stat_activity` can't give. We checked on 18.4, with a 0.5 rate and
  `log_line_prefix = 'qid=%Q user=%u '`:
  - a 0.017 ms statement was logged as readily as slower ones;
  - the full text was logged, leading comment included;
  - `%Q` gave the `queryid`;
  - `%u` is the session user, so the same grouping applies;
  - a multi-statement message is one line.

  At a 0.01 rate, a server doing 10,000 calls/s logs about 100 lines a
  second. The catch is access: the worker would need the server's log, which
  on managed services comes through a provider API, and logging settings
  changed on the observed server. It's the path for **call** counts, if the
  user can give the worker the logs (open question 2).
- **Unsampled logging (`log_min_duration_statement = 0`).** Exact, but it
  logs every statement, with the I/O and volume that brings.
- **`auto_explain`.** Logs plans for slow statements, optionally sampled.
  It has the same log access problem, costs more per call, and covers only
  slow statements.
- **Extensions such as `pg_stat_monitor`.** It can keep comments per bucket,
  which gives exact per-call context. But it's a third-party extension that
  many managed services don't offer, its time buckets would replace our
  snapshot diffing, and we'd need a second reader beside pgss.
- **App-side metrics** (Active Support notifications, OpenTelemetry). Exact,
  but they're per-app work outside rotten, and app SQL doesn't map to our
  fingerprints without the same fingerprinting. Good for spot checks.
- **A separate sampling connection.** It would avoid the harvest gaps, but
  there'd be two connections to pin to one node, and twice the identity
  checks. Gaps are cheaper to count than to remove.
- **Doing nothing.** Honest with the caveat, but the error stays invisible.

## Is this worth building?

**What it buys.**

- Stage 1 makes the invisible error visible. When a first-text count is
  mixed, the UI names the contexts that ran the statement without getting
  credit. That's cheap and makes no statistical claim.
- Stage 2 gives a rough activity share for busy, complete groups on stable
  servers. That's mostly the expensive statements, which matter most for
  load and for the replica utilization reports.

**What it doesn't buy.**

- Call counts per context. Those need sampled logging.
- Error bars.
- Shares on 14–16 servers that evict pgss entries often, through
  transaction-mode poolers, for groups with partial picks, for long
  statements that span windows, or for cheap, rare statements. All of
  these fall back.
- Much in dev, where the 10-second window rarely reaches 10 hits.

**What it costs.**

- A sampler with Poisson ticks, deadlines, node checks, truncation and
  multi-statement rules.
- Two proto fields, two tables and new validation.
- A new time view in the UI.
- One opt-in config key, plus a pooler requirement.

Stage 2 is about twice the work of stage 1, for a number we have to label
as rough.

**Verdict.** The value is real but narrow. Stage 1 is worth building: it's
small and turns a silent error into a named one. Stage 2 is worth building
only if, after stage 1 has run on real servers, sightings show many busy
fingerprints with mixed contexts. Stage 1's tick counts tell us how many
groups would qualify.

## Recommendation.

- **Build stage 1:** the opt-in sampler, and sightings shown under the
  context tables. The sampler is built in full (Poisson ticks, deadlines,
  node identity, complete groups, strict truncation and multi-statement
  rules), because stage 2 needs the same parts.
- **Decide on stage 2 after stage 1 has run**, using its tick counts. If it's
  built, it's the activity share described here: splitting `total_time`,
  whole-group fallbacks, the boundary rule, no intervals, and low-data
  flags.
- **Calls stay first text.** If the user wants per-context calls, the next
  design is sampled statement logging.

## Open questions for sign-off.

1. **Stages.** Build stage 1 (sightings) now, and decide on stage 2 (activity
   share) after it has run? Or build both, or neither?
2. **Call counts.** Should we design sampled statement logging for calls?
   That needs the worker to read the observed server's logs, or a provider
   API, and logging settings changed on that server.
3. **Opt-in key.** Is one optional key, `ContextSampleInterval`, off by
   default, the right shape? Should it default on later?
4. **Direct connections.** Is it acceptable that sampling needs
   `ObservedDBConn` to reach the server directly, or through a session-mode
   pooler?
5. **Honest labels.** If stage 2 is built: is an "activity share, assuming a
   steady mix" with tick counts and a low-data flag, and no intervals,
   something you'd use? Are 10 ticks and 10 executions as the minimum, and
   low data under 30 ticks, a reasonable start before validation?
6. **Fallbacks.** Is it acceptable that whole groups fall back on any member
   reset, a partial pick, or more than 10% of hits from boundary executions,
   even though that makes shares rare on some servers?
7. **Caps.** Are 3 sightings and 8 time rows per fingerprint, and 6,000 rows
   per harvest, acceptable?

## Proposed task breakdown.

The IDs follow the backlog's form. The main session adds these to
`BACKLOG.md` after sign-off. Every task starts with its red test. Tasks 4
through 7 are stage 2, and wait on open question 1.

### 20261004-204000-1: Sample pg_stat_activity in the worker.

- **Needs:** sign-off on open questions 1, 3 and 4.
- **Do:** Add `ContextSampleInterval` (100–60,000 ms; absent or 0 = off).
  Run Poisson ticks between harvests on the harvest connection, each as a
  pgx batch with `set local statement_timeout = '250ms'` and a 1 s Go
  deadline. Skip a tick when a harvest is due within 250 ms. Record gaps.
  Check node identity on every tick and harvest. Take the post-read tick to
  find boundary executions. Read `track_activity_query_size` when
  connecting. Apply the truncation and multi-statement rules. Group hits by
  `(dbid, queryid)`, and apply the complete-group rule. Document the key
  and the pooler requirement in `docs/worker.md` and `conf`, and turn it on
  in `dev/`.
- **Red test:** On real 14 and 18:
  - a running `pg_sleep` with leading and trailing comments is hit with the
    right context;
  - an idle session isn't hit;
  - a 1,022-byte multibyte cut counts as possibly truncated;
  - a tagged-plus-untagged multi-statement message is unknown;
  - a multi-statement message whose trailing comment is cut by truncation
    is unknown;
  - a `SET ROLE` session lands in its group;
  - with two users in a group and only one picked, the group is
    incomplete;
  - a reconnect drops the window;
  - a tick blocked past 250 ms is cancelled, counted as a gap, and doesn't
    delay the harvest by more than its deadline.

### 20261004-204000-2: Send and store sightings.

- **Needs:** 20261004-204000-1.
- **Do:** Add `FingerprintAggregate.sampling` (6), `ContextSampling` and
  `SeenContext`. Add the 3-per-aggregate cap and its validation to
  `internal/harvestlimits`. Add the `event_context_sighting` migration with
  its grants, and write it in `internal/ingest`. Apply the lifecycle rules
  that cover sightings (baseline, node change, incomplete group).
- **Red test:** An ingest round trip. A boundary test where 3 sightings are
  accepted and 4 rejected. An old-server decode test showing the field is
  skipped, and the batch accepted.

### 20261004-204000-3: Show sightings in the UI, and update the Postgres 18 warning.

- **Needs:** 20261004-204000-2.
- **Do:** Show "Sampling also saw this statement running from: …" under
  the context tables, with the caveat. When ticks see contexts that pgss
  texts don't, change the no-contexts warning (`context_warning.go`) to say
  sampling sees the leading comments. Update `ui/README.md`,
  `docs/worker.md` and the docscheck that pins the caveat.
- **Red test:** A system spec with a fingerprint that has sightings, and
  one without. A `context_warning` test where ticks match but pgss texts
  don't.

### 20261004-204000-4: Carry time attribution through the proto, limits, ingest and schema.

- **Needs:** the user's go-ahead on stage 2 (open question 1),
  20261004-204000-2.
- **Do:** Add `FingerprintAggregate.time_attribution` (7) and
  `TimeAttribution`. Add the 8-row per-aggregate cap, the shared 6,000-row
  per-harvest cap and the validation. Add the `event_time_attribution`
  migration with its grants, and write it in `internal/ingest`.
- **Red test:** A boundary test where 6,000 combined rows are accepted and
  6,001 rejected, and 9 rows on one aggregate are rejected. A worst-case
  encoded batch stays under `MaxIngestMessageBytes`. A batch whose times
  don't add up to `total_time` is rejected.

### 20261004-204000-5: Split each group's total time and build the time attribution.

- **Needs:** 20261004-204000-1, 20261004-204000-4.
- **Do:** Apply the qualifying rules: 10 ticks and 10 executions, at most
  10% boundary hits, gaps of at most 10%, no member reset or recreated, and
  a complete group. Split `total_time` (planning plus execution), keeping
  unknown time as unattributed, and use first text otherwise. Build rows by
  fingerprint, fold to 8 rows, and trim per harvest.
- **Red test:** Unit tests:
  - entries with nonzero planning time split `total_time`, and the rows add
    up to it;
  - unknown hits stay in the denominator;
  - a group with 9 ticks falls back;
  - with two users in a group and only one recreated, the whole group falls
    back;
  - a group with more than 10% boundary hits falls back;
  - a fingerprint with 4 sampled and 6 first-text contexts gives 8 rows,
    with "other";
  - a fingerprint where every group falls back sends no time attribution;
  - a harvest over 6,000 rows drops the smallest aggregates' time
    attribution.

### 20261004-204000-6: Validate the activity share on real Postgres.

- **Needs:** 20261004-204000-5.
- **Do:** Build the validation plan as integration tests on 14 and 18.
  Record the results in this doc. Set the low-data and minimum thresholds
  from them.
- **Red test:** The unequal-latency, periodic, concurrent-burst,
  long-statement, planning-time, truncation and harvest-pause cases.

### 20261004-204000-7: Show the activity share in reports and the UI.

- **Needs:** 20261004-204000-4, 20261004-204000-6.
- **Do:** Have the time-based reports (the context time columns and the
  replica utilization reports) read `event_time_attribution` when present.
  Show `≈ share (ticks)` and the first-text, unattributed, other and
  low-data labels, with the steady-mix caveat. Keep call counts and their
  caveat.
- **Red test:** A system spec with sampled, first-text, unattributed and
  low-data rows. A report spec where an event with time attribution uses
  it, and one without uses `attributed_time`.

### 20261004-204000-8: Design call attribution by sampled statement logging.

- **Needs:** the user's answer to open question 2.
- **Do:** Write a decision doc on reading
  `log_min_duration_sample`/`log_statement_sample_rate` output (with `%Q`)
  to estimate per-context calls. Cover log access (local file or provider
  API), the cost at a given rate, and how it fits with sightings and the
  activity share.
- **Red test:** A docscheck that the doc exists and covers those points.

## Changes after review.

**Round 2.** The reframe to time share was accepted. The direction was to
simplify and claim less.

| # | Finding | Resolution |
| --- | --- | --- |
| 1 | The interval treated simultaneous executions as independent | Intervals and variance dropped. Shows tick and execution counts, a 10-tick minimum and a low-data flag under 30, set from validation, including a concurrent-burst case. |
| 2 | Group shares were applied to entries picked one by one | Shares (and sightings) used only for complete groups, where every member was picked and all share one fingerprint. Otherwise the whole group falls back. Two-user partial-pick test. |
| 3 | The split covered exec time, but `total_time` includes planning | Split `metrics.total_time` (planning plus execution), assuming planning follows the same share. `active` covers planning too. Nonzero-planning test. |
| 4 | Multi-statement rules credited ambiguous contexts | Credit only when every statement has the identical context, including all empty. A truncated multi-statement message is unknown. Tests for both. |
| 5 | Harvest pauses made the unbiased claim false | Pauses counted as gaps, with no unbiased claim, and the window's shares unused over 10% gaps. Per-tick `statement_timeout` of 250 ms in a tick-only transaction (the harvest has none), a 1 s Go deadline, and no tick within 250 ms of a harvest. Separate connection considered and rejected. |
| 6 | In-window activity isn't completed-call deltas | Labelled an activity share under a steady-mix assumption, with a boundary-execution rule (over 10% of hits means falling back), found with one extra tick per harvest. Long-statement validation case. |
| 7 | The newest reset in a group doesn't align every member | Any member reset or recreated in the window means the whole group falls back. A global reset means all groups fall back. Two-user test. |
| 8 | The 8-row cap ignored first-text rows | 8 rows in total across all kinds, with folding into "other" defined. No time attribution when every group falls back. Tests for mixed and all-fallback fingerprints. |

Beyond the findings, the design is now staged. Sightings (stage 1) ship
first, and the activity share (stage 2) waits on evidence from stage 1. The
section [Is this worth building?](#is-this-worth-building) was added.

**Round 1.**

| # | Finding | Resolution |
| --- | --- | --- |
| 1 | Samples are duration-biased but were presented as call estimates | Reframed as a time share, measured on 18.4. Poisson ticks against phase lock. Sampled logging named as the call method. |
| 2 | Merged confidence pooled raw samples | Superseded: intervals are dropped (round 2, #1). Shares are still combined with per-group time weights, never by pooling hits, and never across windows. |
| 3 | Dropping unknown truncated samples could erase a context | Unknown ticks stay in the denominator as unattributed time. Context-dependent truncation test. |
| 4 | `(pid, query_start)` isn't one execution | Verified on 14, 17 and 18. Ticks keyed by `query_id`. Multi-statement handling tightened in round 2, #4. |
| 5 | `usesysid` isn't pgss `userid` under `SET ROLE` | Verified on 14, 17 and 18. Group by `(dbid, queryid)`. Complete-group rule added in round 2, #2. |
| 6 | Sampler and harvester may reach different nodes | Ticks on the harvest connection, `pg_backend_pid()` and `pg_postmaster_start_time()` checked on every tick and harvest, and direct or session-mode connections required. |
| 7 | Splitting breaks `MaxHarvestContexts` | New fields with their own shared caps, worker trimming, a byte budget, and old-server compatibility. |
| 8 | Exact byte-length truncation misses multibyte cuts | Verified a 1,022-byte cut. A 4-byte margin on `octet_length`, and matching only up to the last complete `*/`. |
| 9 | Samples weren't invalidated on recreation | Sample lifecycle table, made whole-group in round 2, #7. |
