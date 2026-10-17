-- Stats for one fingerprint on each source (project, environment, cluster
-- and role) in a source filter: its calls and time in an observed time range, plus the
-- per-call mean_time history the ingest server keeps in fingerprint_stats.
--
-- Parameters:
--   $1 projects (text[]), or NULL for every project
--   $2 environments (text[]), or NULL for every environment
--   $3 clusters (text[]), or NULL for every cluster
--   $4 fingerprint_id
--   $5 window start, inclusive
--   $6 window end, exclusive for starts; windows must also end at or before it
--   $7 roles (text[]), or NULL for every role in the sources above
--   $8 contexts (text[]): "controller#action#job_tag", empty parts for
--      none, or NULL for every context. With it, the report counts only the
--      calls (event_context.c) and time (attributed_time) in these contexts.
--   $9 ignore untagged (boolean): leave out the untagged context's calls and
--      time.
--      The history columns are the fingerprint's on the source either way.
--
-- One row per logical source that has windows in the range or history; a
-- source with neither is left out. calls and total_ms are 0 and
-- avg_ms_per_call is NULL for a source with history but no windows in the
-- range. The history columns are NULL for a source with no mean_time row.
-- Only windows fully inside [$5, $6) are counted; straddling windows are
-- excluded on purpose, the same as the sibling reports.
with sources as (
  select id, project, environment, cluster, role
  from rotten.logical_sources
  where ($1::text[] is null or project = any($1::text[]))
    and ($2::text[] is null or environment = any($2::text[]))
    and ($3::text[] is null or cluster = any($3::text[]))
    and ($7::text[] is null or role = any($7::text[]))
), wanted as (
  -- $8's contexts as dimension ids. Each is "controller#action#job_tag" with
  -- empty parts for none (so "##" is the untagged context); the job tag,
  -- last, may hold a #. A context naming a controller, action or job tag no
  -- row has can't match anything, so it's dropped.
  select c.id as controller_id, ac.id as action_id, jt.id as job_tag_id
  from unnest($8::text[]) as w(context)
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
), in_range as (
  select
    e.logical_source_id,
    sum(e.calls)::double precision as calls,
    sum(e.time)::double precision as total_ms
  from rotten.events e
  join sources s on s.id = e.logical_source_id
  where e.fingerprint_id = $4::bigint
    and e.observed_window_start >= $5::timestamptz
    and e.observed_window_start < $6::timestamptz
    and e.observed_window_end <= $6::timestamptz
    and not ($8::text[] is not null or $9::boolean)
  group by e.logical_source_id
  union all
  -- With $8 or $9, the calls and time event_context attributes to the kept
  -- contexts instead.
  select
    ec.logical_source_id,
    sum(ec.c)::double precision as calls,
    sum(coalesce(ec.attributed_time, 0))::double precision as total_ms
  from rotten.events e
  join sources s on s.id = e.logical_source_id
  join rotten.event_context ec on ec.event_id = e.id and ec.observed_window_start = e.observed_window_start
  where ($8::text[] is not null or $9::boolean)
    and e.fingerprint_id = $4::bigint
    and e.observed_window_start >= $5::timestamptz
    and e.observed_window_start < $6::timestamptz
    and e.observed_window_end <= $6::timestamptz
    and ec.observed_window_start >= $5::timestamptz
    and ec.observed_window_start < $6::timestamptz
    and ($8::text[] is null or exists (
      select 1 from wanted w
      where w.controller_id is not distinct from ec.controller_id
        and w.action_id is not distinct from ec.action_id
        and w.job_tag_id is not distinct from ec.job_tag_id))
    and not ($9::boolean and ec.controller_id is null and ec.action_id is null and ec.job_tag_id is null)
  group by ec.logical_source_id
), history as (
  select fs.logical_source_id, fs.count, fs.mean, fs.deviation
  from rotten.fingerprint_stats fs
  join sources s on s.id = fs.logical_source_id
  where fs.fingerprint_id = $4::bigint
    and fs.type = 'mean_time'
)
select
  s.project,
  s.environment,
  s.cluster,
  s.role,
  coalesce(r.calls, 0)::double precision as calls,
  coalesce(r.total_ms, 0)::double precision as total_ms,
  (r.total_ms / nullif(r.calls, 0))::double precision as avg_ms_per_call,
  h.count as history_samples,
  h.mean as history_mean_ms,
  h.deviation as history_deviation_ms
from sources s
left join in_range r on r.logical_source_id = s.id
left join history h on h.logical_source_id = s.id
where r.logical_source_id is not null
   or h.logical_source_id is not null
order by s.project, s.environment, s.cluster, s.role;
