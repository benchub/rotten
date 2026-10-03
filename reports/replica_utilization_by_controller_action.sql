-- Primary/replica utilization by controller/action for a source filter and observed time range.
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
--   This report returns both call utilization and time utilization. Calls are
--   attributed by event_context.c, matching the top-query reports. Time is
--   split across all of an event's context rows as event time * c / ctx_total;
--   ctx_total is computed before filtering to controller/action contexts.
--
-- Percentages:
--   The primary percentage is role_value / primary_plus_replica_value * 100,
--   rounded to two decimal places. The replica percentage is 100 - primary, so
--   the two displayed percentages add to 100. Zero denominators produce 0/0.
--
-- Ordering:
--   Rows are ordered by total calls descending, then total time descending,
--   then controller/action name and cluster for deterministic output.
with sources as (
  select id, cluster, role
  from rotten.logical_sources
  where project = $1
    and environment = $2
    and cluster = $3
    and role in ($6, $7)
), event_context_with_totals as (
  select
    ec.*,
    sum(ec.c) over (partition by ec.event_id) as ctx_total
  from rotten.event_context ec
  where ec.observed_window_start >= $4::timestamptz
    and ec.observed_window_start < $5::timestamptz
    and ec.observed_window_end <= $5::timestamptz
), aggregated_events as (
  select
    s.cluster,
    s.role,
    ec.controller_id,
    ec.action_id,
    sum(ec.c)::bigint as calls,
    sum(e.time * ec.c::double precision / ec.ctx_total)::double precision as total_ms
  from rotten.events e
  join sources s on s.id = e.logical_source_id
  join event_context_with_totals ec on ec.event_id = e.id
  -- Only windows fully inside [start, end) are counted; straddling windows are excluded on purpose.
  where e.observed_window_start >= $4::timestamptz
    and e.observed_window_start < $5::timestamptz
    and e.observed_window_end <= $5::timestamptz
    and ec.observed_window_start >= $4::timestamptz
    and ec.observed_window_start < $5::timestamptz
    and ec.observed_window_end <= $5::timestamptz
    and (ec.controller_id is not null or ec.action_id is not null)
    and ec.ctx_total > 0
  group by s.cluster, s.role, ec.controller_id, ec.action_id
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
    coalesce(p.controller_id, r.controller_id) as controller_id,
    coalesce(p.action_id, r.action_id) as action_id,
    coalesce(p.calls, 0)::bigint as primary_calls,
    coalesce(r.calls, 0)::bigint as replica_calls,
    coalesce(p.total_ms, 0)::double precision as primary_total_ms,
    coalesce(r.total_ms, 0)::double precision as replica_total_ms
  from primary_events p
  full outer join replica_events r on r.cluster = p.cluster
    and r.controller_id is not distinct from p.controller_id
    and r.action_id is not distinct from p.action_id
), totals as (
  select
    c.*,
    (c.primary_calls + c.replica_calls)::bigint as total_calls,
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
  coalesce(ctrl.controller, '') || '#' || coalesce(act.action, '') as controller_action,
  t.cluster,
  t.primary_calls,
  t.replica_calls,
  t.total_calls,
  t.primary_call_percent::double precision as primary_call_percent,
  case when t.total_calls > 0 then (100::numeric - t.primary_call_percent)::double precision else 0::double precision end as replica_call_percent,
  t.primary_total_ms,
  t.replica_total_ms,
  t.total_ms,
  t.primary_time_percent::double precision as primary_time_percent,
  case when t.total_ms > 0 then (100::numeric - t.primary_time_percent)::double precision else 0::double precision end as replica_time_percent
from totals t
left join rotten.controllers ctrl on ctrl.id = t.controller_id
left join rotten.actions act on act.id = t.action_id
order by total_calls desc, total_ms desc, controller_action, t.cluster;
