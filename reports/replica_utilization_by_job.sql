-- Primary/replica utilization by job tag for a source filter and observed time range.
--
-- Parameters:
--   $1 projects (text[]), or NULL for every project
--   $2 environments (text[]), or NULL for every environment
--   $3 clusters (text[]), or NULL for every cluster
--   $4 window start, inclusive
--   $5 window end, exclusive for starts; windows must also end at or before it
--   $6 primary role name
--   $7 replica role name
--   $8 match: case-insensitive POSIX regexes (~*, text[]) on the job tag, OR'd,
--      or NULL for every row. The untagged row never matches a pattern.
--   $9 exclude: regexes (text[]) on the job tag, or NULL for none. A row any
--      of them matches is dropped before match applies.
--   $10 contexts (text[]): "controller#action#job_tag", empty parts for
--      none, or NULL for every context. With it, the report counts only the
--      calls (event_context.c) and time (attributed_time) in these contexts.
--   $11 ignore untagged (boolean): leave out the untagged context's calls and
--      time: the untagged row goes.
--      With $10, match ($8) and exclude ($9) see only the rows left, so only
--      the filtered contexts.
--
-- Rows:
--   One row per source (project, environment and cluster, returned as the
--   last columns) and job tag. Untagged calls, context rows with no controller,
--   action or job tag, are one row of their own with job_tag NULL and
--   untagged true: calls pg_stat_statement_context didn't attribute, and
--   every call from a database without it. Rows with a controller or action
--   but no job tag aren't here (they're in the controller/action report).
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
--   then job tag project, environment and cluster for deterministic output.
with sources as (
  select id, project, environment, cluster, role
  from rotten.logical_sources
  where ($1::text[] is null or project = any($1::text[]))
    and ($2::text[] is null or environment = any($2::text[]))
    and ($3::text[] is null or cluster = any($3::text[]))
    and role in ($6, $7)
), wanted as (
  -- $10's contexts as dimension ids. Each is "controller#action#job_tag" with
  -- empty parts for none (so "##" is the untagged context); the job tag,
  -- last, may hold a #. A context naming a controller, action or job tag no
  -- row has can't match anything, so it's dropped.
  select c.id as controller_id, ac.id as action_id, jt.id as job_tag_id
  from unnest($10::text[]) as w(context)
  cross join lateral (
    select
      nullif(split_part(w.context, '#', 1), '') as controller,
      nullif(split_part(w.context, '#', 2), '') as action,
      nullif(substring(w.context from '^[^#]*#[^#]*#(.*)$'), '') as job_tag
  ) p
  left join rotten.controllers c on c.controller = p.controller
  left join rotten.actions ac on ac.action = p.action
  left join rotten.job_tags jt on jt.job_tag = p.job_tag
  where (p.controller is null) = (c.id is null)
    and (p.action is null) = (ac.id is null)
    and (p.job_tag is null) = (jt.id is null)
), aggregated_events as (
  select
    s.project,
    s.environment,
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
    and ($10::text[] is null or exists (
      select 1 from wanted w
      where w.controller_id is not distinct from ec.controller_id
        and w.action_id is not distinct from ec.action_id
        and w.job_tag_id is not distinct from ec.job_tag_id))
    and not ($11::boolean and ec.controller_id is null and ec.action_id is null and ec.job_tag_id is null)
    and (ec.job_tag_id is not null
      or (ec.controller_id is null and ec.action_id is null))
  group by s.project, s.environment, s.cluster, s.role, ec.job_tag_id
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
    coalesce(p.project, r.project) as project,
    coalesce(p.environment, r.environment) as environment,
    coalesce(p.cluster, r.cluster) as cluster,
    coalesce(p.job_tag_id, r.job_tag_id) as job_tag_id,
    coalesce(p.calls, 0)::numeric as primary_calls,
    coalesce(r.calls, 0)::numeric as replica_calls,
    coalesce(p.total_ms, 0)::double precision as primary_total_ms,
    coalesce(r.total_ms, 0)::double precision as replica_total_ms
  from primary_events p
  full outer join replica_events r on r.project = p.project
    and r.environment = p.environment
    and r.cluster = p.cluster
    and r.job_tag_id is not distinct from p.job_tag_id
), totals as (
  select
    c.*,
    (c.job_tag_id is null) as untagged,
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
  case when t.total_ms > 0 then (100::numeric - t.primary_time_percent)::double precision else 0::double precision end as replica_time_percent,
  t.untagged,
  t.project,
  t.environment
from totals t
left join rotten.job_tags jt on jt.id = t.job_tag_id
-- $9 drops a row whose name an exclude pattern matches; then $8 keeps a row
-- whose name a match pattern matches. The untagged row has no name, so
-- neither matches it.
where not coalesce(not t.untagged and jt.job_tag ~* any($9::text[]), false)
  and ($8::text[] is null or (not t.untagged and jt.job_tag ~* any($8::text[])))
order by total_calls desc, total_ms desc, jt.job_tag, t.project, t.environment, t.cluster;
