-- Calls and time for one fingerprint and one context over time, for a source
-- filter. The same buckets as fingerprint_timeseries.sql, counting only that
-- context's share.
--
-- Parameters:
--   $1 project
--   $2 environment
--   $3 cluster
--   $4 fingerprint_id
--   $5 window start, inclusive
--   $6 window end, exclusive for starts; windows must also end at or before it
--   $7 bucket width interval
--   $8 role, or NULL for every role
--   $9 context controller, or NULL
--   $10 context action, or NULL
--   $11 context job tag, or NULL
--
-- Context:
--   A context is the (controller, action, job tag) triple the contexts
--   report groups by, and it matches only that exact triple: a NULL part
--   matches only rows where that part is null. All three NULL is the
--   untagged context. A context the fingerprint never ran in gives all-zero
--   buckets rather than an error.
--
-- Calls and time:
--   calls is the sum of event_context.c. total_ms is the sum of
--   event_context.attributed_time, an estimate of the context's share of its
--   event's time (see the column comment, migrations 0011 and 0015): the
--   shares of one event add up to its time, but the context's own time isn't
--   measured exactly. Rows ingested before the repair has filled
--   attributed_time count as 0 ms.
--
-- Buckets, source filter, limits and window semantics are those of
-- fingerprint_timeseries.sql. Events come from events_source_fingerprint_window
-- (migration 0013), then their event_context rows by event_id, with
-- observed_window_start filters on both tables so the planner prunes
-- partitions.
with input as (
  select
    $5::timestamptz as start_at,
    $6::timestamptz as end_at,
    make_interval(secs => extract(epoch from $7::interval)) as width,
    case
      when extract(year from $7::interval) <> 0
        or extract(month from $7::interval) <> 0
        then ('fingerprint_context_timeseries bucket width must not contain months or years: ' || $7::text)::integer
      when extract(epoch from $7::interval) <= 0
        then ('fingerprint_context_timeseries bucket width must be positive: ' || $7::text)::integer
      when $6::timestamptz <= $5::timestamptz then 1
      when ceil(extract(epoch from ($6::timestamptz - $5::timestamptz)) / extract(epoch from $7::interval)) > 10000
        then ('fingerprint_context_timeseries bucket count must be at most 10000: ' || $7::text)::integer
      else 1
    end as ok
), sources as (
  select id
  from rotten.logical_sources
  where project = $1
    and environment = $2
    and cluster = $3
    and ($8::text is null or role = $8::text)
), buckets as (
  select
    gs.bucket_start,
    least(gs.bucket_start + i.width, i.end_at) as bucket_end
  from input i
  cross join lateral generate_series(
    i.start_at,
    i.end_at - interval '1 microsecond',
    i.width
  ) as gs(bucket_start)
  where i.ok = 1
    and i.end_at > i.start_at
), matched as (
  select e.id
  from rotten.events e
  join sources s on s.id = e.logical_source_id
  where e.fingerprint_id = $4::bigint
    and e.observed_window_start >= $5::timestamptz
    and e.observed_window_start < $6::timestamptz
    and e.observed_window_end <= $6::timestamptz
), aggregated as (
  select
    date_bin(i.width, ec.observed_window_start, i.start_at) as bucket_start,
    sum(ec.c)::bigint as calls,
    sum(coalesce(ec.attributed_time, 0))::double precision as total_ms
  from matched m
  cross join input i
  join rotten.event_context ec on ec.event_id = m.id
  left join rotten.controllers c on c.id = ec.controller_id
  left join rotten.actions ac on ac.id = ec.action_id
  left join rotten.job_tags jt on jt.id = ec.job_tag_id
  where i.ok = 1
    and ec.observed_window_start >= $5::timestamptz
    and ec.observed_window_start < $6::timestamptz
    and ec.observed_window_end <= $6::timestamptz
    and c.controller is not distinct from $9::text
    and ac.action is not distinct from $10::text
    and jt.job_tag is not distinct from $11::text
  group by date_bin(i.width, ec.observed_window_start, i.start_at)
)
select
  b.bucket_start,
  b.bucket_end,
  coalesce(a.calls, 0)::bigint as calls,
  coalesce(a.total_ms, 0)::double precision as total_ms
from buckets b
left join aggregated a on a.bucket_start = b.bucket_start
order by b.bucket_start;
