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
--      The untagged context never matches: it has no controller, action or
--      job tag.
--
-- Contexts:
--   context lists a fingerprint's top five contexts by calls. The untagged
--   context, calls pg_stat_statement_context didn't attribute and every call
--   from a database without it, is one with controller, action and job_tag
--   all null.
with sources as (
  select id
  from rotten.logical_sources
  where project = $1
    and environment = $2
    and cluster = $3
    and ($7::text is null or role = $7::text)
), context_events as (
  -- In-range events with a context, grouped by context, so $8 runs once per
  -- distinct controller, action and job tag rather than once per
  -- event_context row. One read of event_context; a first pass for the
  -- distinct contexts and a second for their events reads it twice.
  select ec.controller_id, ec.action_id, ec.job_tag_id, array_agg(ec.event_id) as event_ids
  from rotten.event_context ec
  join sources s on s.id = ec.logical_source_id
  where $8::text is not null
    and ec.observed_window_start >= $4::timestamptz
    and ec.observed_window_start < $5::timestamptz
    and ec.observed_window_end <= $5::timestamptz
    and (ec.controller_id is not null or ec.action_id is not null or ec.job_tag_id is not null)
  group by ec.controller_id, ec.action_id, ec.job_tag_id
), matched_events as materialized (
  -- Events in the range with a context matching $8: controller#action, or
  -- job tag. Empty when $8 is NULL. Materialized so the left join below
  -- probes one hash of it, built once.
  select distinct u.event_id
  from context_events k
  left join rotten.controllers c on c.id = k.controller_id
  left join rotten.actions ac on ac.id = k.action_id
  left join rotten.job_tags jt on jt.id = k.job_tag_id
  cross join lateral unnest(k.event_ids) as u(event_id)
  where ((k.controller_id is not null or k.action_id is not null)
      and coalesce(c.controller, '') || '#' || coalesce(ac.action, '') ~* $8::text)
    or jt.job_tag ~* $8::text
), aggregated_all as (
  select
    e.fingerprint_id,
    sum(e.calls)::double precision as calls,
    sum(e.time)::double precision as total_ms,
    (sum(e.time) / nullif(sum(e.calls), 0))::double precision as avg_ms_per_call,
    bool_or(m.event_id is not null) as context_matched,
    array_agg(e.id) as event_ids
  from rotten.events e
  join sources s on s.id = e.logical_source_id
  left join matched_events m on m.event_id = e.id
  -- Only windows fully inside [start, end) are counted; straddling windows are excluded on purpose.
  where e.observed_window_start >= $4::timestamptz
    and e.observed_window_start < $5::timestamptz
    and e.observed_window_end <= $5::timestamptz
  group by e.fingerprint_id
), text_matched as (
  -- Fingerprints in the range, without a matching context, whose query text
  -- matches $8. Probed by id, so the fingerprints table isn't scanned. A
  -- correlated lookup per group, in aggregated's HAVING, costs the same at run
  -- time, but the planner prices it per estimated group, which pushes the
  -- plan's cost past jit_optimize_above_cost and adds over a second of JIT.
  select f.id
  from rotten.fingerprints f
  where $8::text is not null
    and f.id = any(array(
      select a.fingerprint_id from aggregated_all a where not a.context_matched))
    and f.normalized ~* $8::text
), aggregated as (
  -- $8 keeps a group only if one of its events ran in a matching context, or
  -- its query text matches. NULL keeps every group.
  select a.fingerprint_id, a.calls, a.total_ms, a.avg_ms_per_call, a.event_ids
  from aggregated_all a
  where $8::text is null
    or a.context_matched
    or a.fingerprint_id in (select id from text_matched)
  order by a.total_ms desc, a.calls desc, a.fingerprint_id
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
  coalesce(ca.contexts, '[]'::jsonb) as context,
  -- A fallback fingerprint hashed from the text; the parser rejected it.
  f.unparsed
from aggregated a
join rotten.fingerprints f on f.id = a.fingerprint_id
left join context_agg ca on ca.fingerprint_id = a.fingerprint_id
order by a.total_ms desc, a.calls desc, a.fingerprint_id;
