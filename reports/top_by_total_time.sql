-- Top queries by total time for a source filter and observed time range.
--
-- Parameters:
--   $1 project
--   $2 environment
--   $3 cluster
--   $4 window start, inclusive
--   $5 window end, exclusive for starts; windows must also end at or before it
--   $6 row limit
--   $7 role, or NULL for every role in the project, environment and cluster
--   $8 match: a case-insensitive POSIX regex (~*), or NULL for every query.
--      A query matches if its normalized text does, or if it ran in the range
--      in a context whose controller#action or job tag does. It filters
--      before the row limit.
with sources as (
  select id
  from rotten.logical_sources
  where project = $1
    and environment = $2
    and cluster = $3
    and ($7::text is null or role = $7::text)
), matched_events as (
  -- Events in the range with a context matching $8: controller#action, or
  -- job tag. Empty when $8 is NULL.
  select ec.event_id
  from rotten.event_context ec
  join sources s on s.id = ec.logical_source_id
  left join rotten.controllers c on c.id = ec.controller_id
  left join rotten.actions ac on ac.id = ec.action_id
  left join rotten.job_tags jt on jt.id = ec.job_tag_id
  where $8::text is not null
    and ec.observed_window_start >= $4::timestamptz
    and ec.observed_window_start < $5::timestamptz
    and ec.observed_window_end <= $5::timestamptz
    and (
      ((ec.controller_id is not null or ec.action_id is not null)
        and coalesce(c.controller, '') || '#' || coalesce(ac.action, '') ~* $8::text)
      or jt.job_tag ~* $8::text
    )
), aggregated as (
  select
    e.fingerprint_id,
    array_agg(e.id) as event_ids,
    sum(e.calls)::double precision as calls,
    sum(e.time)::double precision as total_ms,
    (sum(e.time) / nullif(sum(e.calls), 0))::double precision as avg_ms_per_call
  from rotten.events e
  join sources s on s.id = e.logical_source_id
  -- Only windows fully inside [start, end) are counted; straddling windows are excluded on purpose.
  where e.observed_window_start >= $4::timestamptz
    and e.observed_window_start < $5::timestamptz
    and e.observed_window_end <= $5::timestamptz
  group by e.fingerprint_id
  -- $8 keeps a group only if one of its events ran in a matching context, or
  -- its query text matches. NULL keeps every group.
  having $8::text is null
    or bool_or(e.id in (select event_id from matched_events))
    or (select f.normalized ~* $8::text from rotten.fingerprints f where f.id = e.fingerprint_id)
  order by total_ms desc, calls desc, e.fingerprint_id
  limit $6::integer
), contexts as (
  select
    a.fingerprint_id,
    sum(ec.c) as times,
    c.controller,
    ac.action,
    jt.job_tag,
    row_number() over (
      partition by a.fingerprint_id
      order by sum(ec.c) desc, coalesce(c.controller, ''), coalesce(ac.action, ''), coalesce(jt.job_tag, '')
    ) as r
  from aggregated a
  join rotten.event_context ec on ec.event_id = any(a.event_ids)
  left join rotten.controllers c on c.id = ec.controller_id
  left join rotten.actions ac on ac.id = ec.action_id
  left join rotten.job_tags jt on jt.id = ec.job_tag_id
  where ec.observed_window_start >= $4::timestamptz
    and ec.observed_window_start < $5::timestamptz
    and ec.observed_window_end <= $5::timestamptz
  group by a.fingerprint_id, c.controller, ac.action, jt.job_tag
), context_agg as (
  select
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
  group by fingerprint_id
)
select
  a.fingerprint_id,
  a.calls,
  a.total_ms,
  a.avg_ms_per_call,
  left(regexp_replace(f.normalized, '\n', ' ', 'g'), 250) as example,
  coalesce(ca.contexts, '[]'::jsonb) as context
from aggregated a
join rotten.fingerprints f on f.id = a.fingerprint_id
left join context_agg ca on ca.fingerprint_id = a.fingerprint_id
order by a.total_ms desc, a.calls desc, a.fingerprint_id;
