-- Queries with a window in the range much slower per call than their own
-- history on the same source.
--
-- Parameters:
--   $1 project
--   $2 environment
--   $3 cluster
--   $4 window start, inclusive
--   $5 window end, exclusive for starts; windows must also end at or before it
--   $6 row limit
--   $7 score threshold, in robust standard deviations (recommended default: 3)
--   $8 minimum history samples before the range (recommended default: 30)
--   $9 minimum slowdown: a window must also be more than this many times the
--      history's median (recommended default: 2)
--   $10 role, or NULL for every role in the project, environment and cluster
--   $11 match: a case-insensitive POSIX regex (~*), or NULL for every query.
--      A query matches if its normalized text does, or if it ran in the range
--      on the same source in a context whose controller#action or job tag does. It filters
--      before the row limit.
--
-- Samples:
--   A sample is one events row: one host's mean milliseconds per call,
--   time / calls, for one fingerprint in one worker window. Each logical
--   source (for example primary or replica) is compared only with its own
--   samples.
--
-- History:
--   The fingerprint's samples on the same source in the lookback before the
--   range: windows that start at or after $4 - lookback and end at or before
--   $4. The lookback is the range's length, clamped to between 1 and 7 days,
--   so a short range still has a day of history and a long one doesn't scan
--   weeks (events are kept 21 days anyway). A fingerprint with fewer than $8
--   samples there, such as an hourly job before a short range, looks further
--   back, up to 7 days before $4, and takes its most recent older windows
--   until it has $8 samples. All the samples of the window that reaches $8
--   count, so it may get a few more. A fingerprint that already has $8 keeps
--   just the lookback, and one with fewer than $8 even in 7 days isn't
--   scored. Only samples before the range count, never later ones, so a
--   later slow spell can't mask this one, and a past range scores the same
--   however much data has arrived since. It reads
--   events only, not fingerprint_stats, so the legacy worker's delayed fingerprint_stats flush doesn't matter.
--
-- Baseline:
--   median = percentile_cont(0.5) of the history samples
--   MAD    = percentile_cont(0.5) of |sample - median|
--   spread = greatest(1.4826 × MAD, ($9 - 1) / $7 × median, 0.01 ms)
--   1.4826 × MAD estimates the standard deviation of normal data, but unlike
--   the standard deviation it ignores past slow spells: up to half the history
--   can be slow without moving it. The floors keep a flat history (MAD near
--   zero) from turning tiny jitter into huge scores. The ratio floor means a
--   window that clears $7 spreads is always more than $9 times the median,
--   which is the old report's rule for a history with no spread. 0.01 ms
--   covers a median near zero.
--
-- Outlier definition:
--   Each in-range sample is scored (sample - median) / spread. A row is an
--   outlier when it has at least $8 history samples and its worst (slowest)
--   in-range sample scores more than $7. Scoring the worst window, not the
--   range's average, keeps a short spell from being averaged away in a long
--   range. Results are ordered by that score before LIMIT is applied, and
--   report the worst window's start and mean, along with the range's totals.
with sources as (
  select id, project, environment, cluster, role
  from rotten.logical_sources
  where project = $1
    and environment = $2
    and cluster = $3
    and ($10::text is null or role = $10::text)
), context_events as (
  -- In-range events with a context, grouped by context, so $11 runs once per
  -- distinct controller, action and job tag rather than once per
  -- event_context row. One read of event_context; a first pass for the
  -- distinct contexts and a second for their events reads it twice.
  select ec.controller_id, ec.action_id, ec.job_tag_id, array_agg(ec.event_id) as event_ids
  from rotten.event_context ec
  join sources s on s.id = ec.logical_source_id
  where $11::text is not null
    and ec.observed_window_start >= $4::timestamptz
    and ec.observed_window_start < $5::timestamptz
    and ec.observed_window_end <= $5::timestamptz
    and (ec.controller_id is not null or ec.action_id is not null or ec.job_tag_id is not null)
  group by ec.controller_id, ec.action_id, ec.job_tag_id
), matched_events as materialized (
  -- Events in the range with a context matching $11: controller#action, or
  -- job tag. Empty when $11 is NULL. Materialized so the left join below
  -- probes one hash of it, built once.
  select distinct u.event_id
  from context_events k
  left join rotten.controllers c on c.id = k.controller_id
  left join rotten.actions ac on ac.id = k.action_id
  left join rotten.job_tags jt on jt.id = k.job_tag_id
  cross join lateral unnest(k.event_ids) as u(event_id)
  where ((k.controller_id is not null or k.action_id is not null)
      and coalesce(c.controller, '') || '#' || coalesce(ac.action, '') ~* $11::text)
    or jt.job_tag ~* $11::text
), aggregated_all as (
  -- Grouped by the two ids only; the source's text columns are joined after.
  -- Sorting every in-range event by them as well is much slower.
  select
    e.logical_source_id,
    e.fingerprint_id,
    sum(e.calls)::double precision as calls,
    sum(e.time)::double precision as total_ms,
    avg(e.time / e.calls)::double precision as avg_ms_per_call,
    max(e.time / e.calls)::double precision as worst_ms_per_call,
    bool_or(m.event_id is not null) as context_matched
  from rotten.events e
  join sources s on s.id = e.logical_source_id
  left join matched_events m on m.event_id = e.id
  -- Only windows fully inside [start, end) are counted; straddling windows are excluded on purpose.
  where e.observed_window_start >= $4::timestamptz
    and e.observed_window_start < $5::timestamptz
    and e.observed_window_end <= $5::timestamptz
    and e.calls > 0
  group by e.logical_source_id, e.fingerprint_id
), text_matched as (
  -- Fingerprints in the range, without a matching context, whose query text
  -- matches $11. Probed by id, so the fingerprints table isn't scanned. A
  -- correlated lookup per group, in aggregated's HAVING, costs the same at run
  -- time, but the planner prices it per estimated group, which pushes the
  -- plan's cost past jit_optimize_above_cost and adds over a second of JIT.
  select f.id
  from rotten.fingerprints f
  where $11::text is not null
    and f.id = any(array(
      select a.fingerprint_id from aggregated_all a where not a.context_matched))
    and f.normalized ~* $11::text
), aggregated as (
  -- $11 keeps a group only if one of its events ran in a matching context, or
  -- its query text matches. NULL keeps every group.
  select
    a.logical_source_id,
    a.fingerprint_id,
    a.calls,
    a.total_ms,
    a.avg_ms_per_call,
    a.worst_ms_per_call
  from aggregated_all a
  where $11::text is null
    or a.context_matched
    or a.fingerprint_id in (select id from text_matched)
), history as (
  -- Every fingerprint's samples on the sources, read by source and window,
  -- then grouped. Joining to aggregated first would invite one index probe
  -- per fingerprint, which is far slower when most of the source's
  -- fingerprints ran in the range.
  select
    h.logical_source_id,
    h.fingerprint_id,
    array_agg((h.time / h.calls)::double precision) as samples,
    count(*)::integer as n
  from rotten.events h
  join sources s on s.id = h.logical_source_id
  where h.observed_window_start >= $4::timestamptz - least(greatest($5::timestamptz - $4::timestamptz, interval '1 day'), interval '7 days')
    and h.observed_window_start < $4::timestamptz
    and h.observed_window_end <= $4::timestamptz
    and h.calls > 0
    -- With $11, only the kept groups' samples are grouped, so the aggregate
    -- stays in memory.
    and ($11::text is null
      or (h.logical_source_id, h.fingerprint_id) in (select logical_source_id, fingerprint_id from aggregated))
  group by h.logical_source_id, h.fingerprint_id
), short as (
  -- The kept groups with fewer than $8 samples in the lookback. Ranges of 7
  -- days or more already look back 7 days.
  select a.logical_source_id, a.fingerprint_id, h.samples, coalesce(h.n, 0) as n
  from aggregated a
  left join history h on h.logical_source_id = a.logical_source_id and h.fingerprint_id = a.fingerprint_id
  where $5::timestamptz - $4::timestamptz < interval '7 days'
    and coalesce(h.n, 0) < $8::integer
), older as (
  -- The short groups' samples from 7 days before $4 up to the lookback,
  -- ranked newest window first. Read by source and window: the + 0 keeps the
  -- planner, which expects few short groups, from probing the events index
  -- once per group, which reads each source's rows once per group.
  select e.logical_source_id, e.fingerprint_id, (e.time / e.calls)::double precision as ms,
    rank() over (partition by e.logical_source_id, e.fingerprint_id order by e.observed_window_start desc) as n
  from rotten.events e
  join sources s on s.id = e.logical_source_id
  where $5::timestamptz - $4::timestamptz < interval '7 days'
    and e.observed_window_start >= $4::timestamptz - interval '7 days'
    and e.observed_window_start < $4::timestamptz - least(greatest($5::timestamptz - $4::timestamptz, interval '1 day'), interval '7 days')
    and e.observed_window_end <= $4::timestamptz
    and e.calls > 0
    and (e.logical_source_id + 0, e.fingerprint_id + 0) in (select logical_source_id, fingerprint_id from short)
), all_history as (
  -- Groups with $8 samples in the lookback, then short groups topped up with
  -- their newest older windows (rank, so a window is never split) up to $8.
  select logical_source_id, fingerprint_id, samples from history where n >= $8::integer
  union all
  select k.logical_source_id, k.fingerprint_id, array_cat(k.samples, o.samples)
  from short k
  join (
    select k2.logical_source_id, k2.fingerprint_id, array_agg(o.ms) as samples
    from short k2
    join older o on o.logical_source_id = k2.logical_source_id and o.fingerprint_id = k2.fingerprint_id
    where o.n <= $8::integer - k2.n
    group by k2.logical_source_id, k2.fingerprint_id
  ) o on o.logical_source_id = k.logical_source_id and o.fingerprint_id = k.fingerprint_id
  where k.n + cardinality(o.samples) >= $8::integer
), scored as (
  select
    a.logical_source_id,
    a.fingerprint_id,
    a.calls,
    a.total_ms,
    a.avg_ms_per_call,
    a.worst_ms_per_call,
    cardinality(h.samples)::bigint as history_samples,
    m.median_ms as history_median_ms,
    sp.spread_ms as history_spread_ms,
    (a.worst_ms_per_call - m.median_ms) / sp.spread_ms as score
  from aggregated a
  join all_history h on h.logical_source_id = a.logical_source_id
    and h.fingerprint_id = a.fingerprint_id
  cross join lateral (
    select percentile_cont(0.5) within group (order by x) as median_ms
    from unnest(h.samples) x
  ) m
  cross join lateral (
    select percentile_cont(0.5) within group (order by abs(x - m.median_ms)) as mad_ms
    from unnest(h.samples) x
  ) d
  cross join lateral (
    select greatest(
      1.4826 * d.mad_ms,
      ($9::double precision - 1) / nullif($7::double precision, 0) * m.median_ms,
      0.01::double precision
    ) as spread_ms
  ) sp
  where (a.worst_ms_per_call - m.median_ms) / sp.spread_ms > $7::double precision
), limited as (
  select *
  from scored
  order by score desc, worst_ms_per_call desc, total_ms desc, fingerprint_id, logical_source_id
  limit $6::integer
), contexts as (
  select
    l.logical_source_id,
    l.fingerprint_id,
    sum(ec.c) as times,
    c.controller,
    ac.action,
    jt.job_tag,
    row_number() over (
      partition by l.logical_source_id, l.fingerprint_id
      order by sum(ec.c) desc, coalesce(c.controller, ''), coalesce(ac.action, ''), coalesce(jt.job_tag, '')
    ) as r
  from limited l
  -- The limited rows' events, looked up again rather than carried through
  -- aggregated as arrays, which makes every group's aggregate spill to disk.
  cross join lateral (
    select array_agg(e.id) as event_ids
    from rotten.events e
    where e.logical_source_id = l.logical_source_id
      and e.fingerprint_id = l.fingerprint_id
      and e.observed_window_start >= $4::timestamptz
      and e.observed_window_start < $5::timestamptz
      and e.observed_window_end <= $5::timestamptz
      and e.calls > 0
  ) le
  join rotten.event_context ec on ec.event_id = any(le.event_ids)
  left join rotten.controllers c on c.id = ec.controller_id
  left join rotten.actions ac on ac.id = ec.action_id
  left join rotten.job_tags jt on jt.id = ec.job_tag_id
  where ec.observed_window_start >= $4::timestamptz
    and ec.observed_window_start < $5::timestamptz
    and ec.observed_window_end <= $5::timestamptz
  group by l.logical_source_id, l.fingerprint_id, c.controller, ac.action, jt.job_tag
), context_agg as (
  select
    logical_source_id,
    fingerprint_id,
    jsonb_agg(
      jsonb_build_object(
        'times', times,
        'controller', controller,
        'action', action,
        'job_tag', job_tag
      )
      order by times desc, coalesce(controller, ''), coalesce(action, ''), coalesce(job_tag, '')
    ) as contexts
  from contexts
  where r <= 5
  group by logical_source_id, fingerprint_id
)
select
  l.logical_source_id,
  l.fingerprint_id,
  s.project,
  s.environment,
  s.cluster,
  s.role,
  l.calls,
  l.total_ms,
  l.avg_ms_per_call,
  w.worst_window_start,
  l.worst_ms_per_call,
  l.history_samples,
  l.history_median_ms,
  l.history_spread_ms,
  l.score,
  left(regexp_replace(f.normalized, '\n', ' ', 'g'), 250) as example,
  coalesce(ca.contexts, '[]'::jsonb) as context,
  -- A fallback fingerprint hashed from the text; the parser rejected it.
  f.unparsed
from limited l
-- The source's text columns, joined only here: joined in scored, the history
-- aggregate ends up under a nested loop and is rebuilt for each source.
join sources s on s.id = l.logical_source_id
join rotten.fingerprints f on f.id = l.fingerprint_id
-- The worst sample's window, earliest on a tie. Looked up only for the
-- limited rows: carrying it through aggregated would cost every in-range
-- event a sort or an array.
cross join lateral (
  select e.observed_window_start as worst_window_start
  from rotten.events e
  where e.logical_source_id = l.logical_source_id
    and e.fingerprint_id = l.fingerprint_id
    and e.observed_window_start >= $4::timestamptz
    and e.observed_window_start < $5::timestamptz
    and e.observed_window_end <= $5::timestamptz
    and e.calls > 0
    and (e.time / e.calls)::double precision = l.worst_ms_per_call
  order by e.observed_window_start
  limit 1
) w
left join context_agg ca on ca.logical_source_id = l.logical_source_id
  and ca.fingerprint_id = l.fingerprint_id
order by l.score desc, l.worst_ms_per_call desc, l.total_ms desc, l.fingerprint_id, l.logical_source_id;
