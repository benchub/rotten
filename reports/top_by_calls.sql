-- Top queries by call count for a source filter and observed time range.
--
-- Parameters:
--   $1 projects (text[]), or NULL for every project
--   $2 environments (text[]), or NULL for every environment
--   $3 clusters (text[]), or NULL for every cluster
--   $4 window start, inclusive
--   $5 window end, exclusive for starts; windows must also end at or before it
--   $6 row limit
--   $7 roles (text[]), or NULL for every role in the sources above
--   $8 match: case-insensitive POSIX regexes (~*, text[]), OR'd, or NULL for
--      every query.
--      A query matches if its normalized text does, or if it ran in the range
--      in a context whose controller#action or job tag does. It filters
--      before the row limit.
--      The untagged context never matches: it has no controller, action or
--      job tag.
--   $9 exclude: case-insensitive POSIX regexes (text[]), or NULL for none. A
--      query any of them matches, the same way, is dropped before match
--      applies.
--   $10 contexts (text[]): "controller#action#job_tag", empty parts for
--      none, or NULL for every context. With it, the report counts only the
--      calls (event_context.c) and time (attributed_time) in these contexts.
--   $11 ignore untagged (boolean): leave out the untagged context's calls and
--      time, and its row in context lists.
--      With $10, $8 and $9 match a query by its text or by the filtered
--      contexts only, not every context its events ran in.
--
-- Contexts:
--   context lists a fingerprint's top five contexts by calls. The untagged
--   context, calls pg_stat_statement_context didn't attribute and every call
--   from a database without it, is one with controller, action and job_tag
--   all null.
with sources as (
  select id
  from rotten.logical_sources
  where ($1::text[] is null or project = any($1::text[]))
    and ($2::text[] is null or environment = any($2::text[]))
    and ($3::text[] is null or cluster = any($3::text[]))
    and ($7::text[] is null or role = any($7::text[]))
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
), context_events as (
  -- In-range events with a context, grouped by context, so $8 runs once per
  -- distinct controller, action and job tag rather than once per
  -- event_context row. One read of event_context; a first pass for the
  -- distinct contexts and a second for their events reads it twice.
  select ec.controller_id, ec.action_id, ec.job_tag_id, array_agg(ec.event_id) as event_ids
  from rotten.event_context ec
  join sources s on s.id = ec.logical_source_id
  where ($8::text[] is not null or $9::text[] is not null)
    and ec.observed_window_start >= $4::timestamptz
    and ec.observed_window_start < $5::timestamptz
    and ec.observed_window_end <= $5::timestamptz
    and (ec.controller_id is not null or ec.action_id is not null or ec.job_tag_id is not null)
    -- With $10, match and exclude see only the filtered contexts.
    and ($10::text[] is null or exists (
      select 1 from wanted w
      where w.controller_id is not distinct from ec.controller_id
        and w.action_id is not distinct from ec.action_id
        and w.job_tag_id is not distinct from ec.job_tag_id))
    and not ($11::boolean and ec.controller_id is null and ec.action_id is null and ec.job_tag_id is null)
  group by ec.controller_id, ec.action_id, ec.job_tag_id
), context_names as (
  -- Each context's names: controller#action (NULL for a job tag alone) and
  -- job tag.
  select
    k.event_ids,
    case when k.controller_id is not null or k.action_id is not null
      then coalesce(c.controller, '') || '#' || coalesce(ac.action, '') end as controller_action,
    jt.job_tag
  from context_events k
  left join rotten.controllers c on c.id = k.controller_id
  left join rotten.actions ac on ac.id = k.action_id
  left join rotten.job_tags jt on jt.id = k.job_tag_id
), matched_events as materialized (
  -- Events in the range with a context one of $8's patterns matches:
  -- controller#action, or job tag. Empty when $8 is NULL. Materialized so
  -- aggregated_all's IN probes one hash of it, built once. A left join to it
  -- instead lets a generic plan nest-loop over it when many sources make the
  -- per-source event estimate small (task 20261005-150200-1).
  select distinct u.event_id
  from context_names k
  cross join lateral unnest(k.event_ids) as u(event_id)
  where k.controller_action ~* any($8::text[]) or k.job_tag ~* any($8::text[])
), excluded_events as materialized (
  -- The same for $9: events in a context an exclude pattern matches.
  select distinct u.event_id
  from context_names k
  cross join lateral unnest(k.event_ids) as u(event_id)
  where k.controller_action ~* any($9::text[]) or k.job_tag ~* any($9::text[])
), aggregated_all as (
  select
    e.fingerprint_id,
    sum(e.calls)::double precision as calls,
    sum(e.time)::double precision as total_ms,
    (sum(e.time) / nullif(sum(e.calls), 0))::double precision as avg_ms_per_call,
    bool_or(e.id in (select event_id from matched_events)) as context_matched,
    bool_or(e.id in (select event_id from excluded_events)) as context_excluded,
    array_agg(e.id) as event_ids
  from rotten.events e
  join sources s on s.id = e.logical_source_id
  -- Only windows fully inside [start, end) are counted; straddling windows are excluded on purpose.
  where e.observed_window_start >= $4::timestamptz
    and e.observed_window_start < $5::timestamptz
    and e.observed_window_end <= $5::timestamptz
    and not ($10::text[] is not null or $11::boolean)
  group by e.fingerprint_id
  union all
  -- With $10 or $11, the calls and time event_context attributes to the
  -- kept contexts instead. A fingerprint with none of them drops out.
  select
    e.fingerprint_id,
    sum(ec.c)::double precision as calls,
    sum(coalesce(ec.attributed_time, 0))::double precision as total_ms,
    (sum(coalesce(ec.attributed_time, 0)) / nullif(sum(ec.c), 0))::double precision as avg_ms_per_call,
    bool_or(e.id in (select event_id from matched_events)) as context_matched,
    bool_or(e.id in (select event_id from excluded_events)) as context_excluded,
    array_agg(distinct e.id) as event_ids
  from rotten.event_context ec
  join sources s on s.id = ec.logical_source_id
  join rotten.events e on e.id = ec.event_id and e.observed_window_start = ec.observed_window_start
  where ($10::text[] is not null or $11::boolean)
    and ec.observed_window_start >= $4::timestamptz
    and ec.observed_window_start < $5::timestamptz
    and ec.observed_window_end <= $5::timestamptz
    and e.observed_window_start >= $4::timestamptz
    and e.observed_window_start < $5::timestamptz
    and ($10::text[] is null or exists (
      select 1 from wanted w
      where w.controller_id is not distinct from ec.controller_id
        and w.action_id is not distinct from ec.action_id
        and w.job_tag_id is not distinct from ec.job_tag_id))
    and not ($11::boolean and ec.controller_id is null and ec.action_id is null and ec.job_tag_id is null)
  group by e.fingerprint_id
), text_matched as (
  -- Fingerprints in the range, without a matching or excluded context, whose
  -- query text one of $8's patterns matches. Probed by id, so the fingerprints table isn't scanned. A
  -- correlated lookup per group, in aggregated's HAVING, costs the same at run
  -- time, but the planner prices it per estimated group, which pushes the
  -- plan's cost past jit_optimize_above_cost and adds over a second of JIT.
  select f.id
  from rotten.fingerprints f
  where $8::text[] is not null
    and f.id = any(array(
      select a.fingerprint_id from aggregated_all a where not a.context_matched and not a.context_excluded))
    and f.normalized ~* any($8::text[])
), text_excluded as (
  -- Fingerprints in the range, without an excluded context, whose query text
  -- one of $9's patterns matches. Probed by id, as text_matched is.
  select f.id
  from rotten.fingerprints f
  where $9::text[] is not null
    and f.id = any(array(
      select a.fingerprint_id from aggregated_all a where not a.context_excluded))
    and f.normalized ~* any($9::text[])
), aggregated as (
  -- $9 drops a group if one of its events ran in an excluded context, or its
  -- query text matches an exclude pattern. Then $8 keeps a group only if one
  -- of its events ran in a matching context, or its query text matches.
  -- NULL keeps every group.
  select a.fingerprint_id, a.calls, a.total_ms, a.avg_ms_per_call, a.event_ids
  from aggregated_all a
  where not a.context_excluded
    and a.fingerprint_id not in (select id from text_excluded)
    and ($8::text[] is null
      or a.context_matched
      or a.fingerprint_id in (select id from text_matched))
  order by a.calls desc, a.total_ms desc, a.fingerprint_id
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
    -- With $10, only the kept contexts are listed; with $11, not the untagged one.
    and ($10::text[] is null or exists (
      select 1 from wanted w
      where w.controller_id is not distinct from ec.controller_id
        and w.action_id is not distinct from ec.action_id
        and w.job_tag_id is not distinct from ec.job_tag_id))
    and not ($11::boolean and ec.controller_id is null and ec.action_id is null and ec.job_tag_id is null)
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
order by a.calls desc, a.total_ms desc, a.fingerprint_id;
