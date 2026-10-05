-- Primary/replica utilization by job tag for a source filter and observed time range.
--
-- Parameters:
--   $1 project
--   $2 environment
--   $3 cluster
--   $4 window start, inclusive
--   $5 window end, exclusive for starts; windows must also end at or before it
--   $6 primary role name
--   $7 replica role name
--   $8 match: a case-insensitive POSIX regex (~*) on the job tag, or NULL
--      for every row
--
-- Utilization:
--   This report returns both call utilization and time utilization. Call
--   totals are returned as double precision because sums can exceed int64. Calls are
--   attributed by event_context.c, matching the top-query reports. Time is
--   split across all of an event's context rows as event time * c / the sum
--   of the event's c, including contexts without a job tag. Ingest stores
--   that share in event_context.attributed_time, and the event's source in
--   event_context.logical_source_id (migration 0011), so this reads
--   event_context alone.
--
-- Percentages:
--   The primary percentage is role_value / primary_plus_replica_value * 100,
--   rounded to two decimal places. The replica percentage is 100 - primary, so
--   the two displayed percentages add to 100. Zero denominators produce 0/0.
--
-- Ordering:
--   Rows are ordered by total calls descending, then total time descending,
--   then job tag and cluster for deterministic output.
with sources as (
  select id, cluster, role
  from rotten.logical_sources
  where project = $1
    and environment = $2
    and cluster = $3
    and role in ($6, $7)
), aggregated_events as (
  select
    s.cluster,
    s.role,
    ec.job_tag_id,
    sum(ec.c) as calls,
    sum(ec.attributed_time)::double precision as total_ms
  from rotten.event_context ec
  join sources s on s.id = ec.logical_source_id
  -- Only windows fully inside [start, end) are counted; straddling windows are excluded on purpose.
  where ec.observed_window_start >= $4::timestamptz
    and ec.observed_window_start < $5::timestamptz
    and ec.observed_window_end <= $5::timestamptz
    and ec.job_tag_id is not null
  group by s.cluster, s.role, ec.job_tag_id
), primary_events as (
  select *
  from aggregated_events
  where role = $6
), replica_events as (
  select *
  from aggregated_events
  where role = $7
), combined as (
  select
    coalesce(p.cluster, r.cluster) as cluster,
    coalesce(p.job_tag_id, r.job_tag_id) as job_tag_id,
    coalesce(p.calls, 0)::numeric as primary_calls,
    coalesce(r.calls, 0)::numeric as replica_calls,
    coalesce(p.total_ms, 0)::double precision as primary_total_ms,
    coalesce(r.total_ms, 0)::double precision as replica_total_ms
  from primary_events p
  full outer join replica_events r on r.cluster = p.cluster
    and r.job_tag_id = p.job_tag_id
), totals as (
  select
    c.*,
    (c.primary_calls + c.replica_calls)::numeric as total_calls,
    (c.primary_total_ms + c.replica_total_ms)::double precision as total_ms,
    case
      when c.primary_calls + c.replica_calls > 0 then round((100.0 * c.primary_calls / (c.primary_calls + c.replica_calls))::numeric, 2)
      else 0::numeric
    end as primary_call_percent,
    case
      when c.primary_total_ms + c.replica_total_ms > 0 then round((100.0 * c.primary_total_ms / (c.primary_total_ms + c.replica_total_ms))::numeric, 2)
      else 0::numeric
    end as primary_time_percent
  from combined c
)
select
  jt.job_tag,
  t.cluster,
  t.primary_calls::double precision as primary_calls,
  t.replica_calls::double precision as replica_calls,
  t.total_calls::double precision as total_calls,
  t.primary_call_percent::double precision as primary_call_percent,
  case when t.total_calls > 0 then (100::numeric - t.primary_call_percent)::double precision else 0::double precision end as replica_call_percent,
  t.primary_total_ms,
  t.replica_total_ms,
  t.total_ms,
  t.primary_time_percent::double precision as primary_time_percent,
  case when t.total_ms > 0 then (100::numeric - t.primary_time_percent)::double precision else 0::double precision end as replica_time_percent
from totals t
join rotten.job_tags jt on jt.id = t.job_tag_id
where $8::text is null or jt.job_tag ~* $8::text
order by total_calls desc, total_ms desc, jt.job_tag, t.cluster;
