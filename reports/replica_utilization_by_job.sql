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
--
-- Utilization:
--   This report returns both call utilization and time utilization. Call
--   totals are returned as double precision because sums can exceed int64. Calls are
--   attributed by event_context.c, matching the top-query reports. Time is
--   split across all of an event's context rows as event time * c / ctx_total;
--   ctx_total is computed before filtering to job-tagged contexts.
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
), event_context_with_totals as (
  -- ctx_total is over all of an event's context rows. Joining events and
  -- sources first keeps the window to the selected sources' contexts;
  -- totaling every context row in the range made generic plans slow at
  -- 7 days (docs/perf.md).
  select
    s.cluster,
    s.role,
    e.time,
    ec.job_tag_id,
    ec.c,
    sum(ec.c) over (partition by ec.event_id) as ctx_total
  from rotten.events e
  join sources s on s.id = e.logical_source_id
  join rotten.event_context ec on ec.event_id = e.id
    -- Ingest writes each context with its event's window. Saying so lets a
    -- generic plan prune event_context to one partition per event.
    and ec.observed_window_start = e.observed_window_start
  -- Only windows fully inside [start, end) are counted; straddling windows are excluded on purpose.
  where e.observed_window_start >= $4::timestamptz
    and e.observed_window_start < $5::timestamptz
    and e.observed_window_end <= $5::timestamptz
    and ec.observed_window_start >= $4::timestamptz
    and ec.observed_window_start < $5::timestamptz
    and ec.observed_window_end <= $5::timestamptz
), aggregated_events as (
  select
    cluster,
    role,
    job_tag_id,
    sum(c) as calls,
    sum(time * c::double precision / ctx_total)::double precision as total_ms
  from event_context_with_totals
  where job_tag_id is not null
    and ctx_total > 0
  group by cluster, role, job_tag_id
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
order by total_calls desc, total_ms desc, jt.job_tag, t.cluster;
