-- How many fingerprints in the range are fallbacks, hashed from the query
-- text because the worker's parser rejected the statement, and their calls
-- and time. The UI shows it under the top and outlier reports.
--
-- Parameters, as for the top reports:
--   $1 projects (text[]), or NULL for every project
--   $2 environments (text[]), or NULL for every environment
--   $3 clusters (text[]), or NULL for every cluster
--   $4 window start, inclusive
--   $5 window end, exclusive for starts; windows must also end at or before it
--   $6 roles (text[]), or NULL for every role in the sources above
--   $7 contexts (text[]): "controller#action#job_tag", empty parts for
--      none, or NULL for every context. With it, the report counts only the
--      calls (event_context.c) and time (attributed_time) in these contexts.
--   $8 ignore untagged (boolean): leave out the untagged context's calls and
--      time.
--      fingerprints then counts the unparsed ones with such calls.
--
-- Unparsed fingerprints are rare, so this starts from them (the partial
-- index fingerprints_unparsed) and reads their events by source and
-- fingerprint through events_source_fingerprint_window, rather than
-- scanning the range.
with sources as (
  select id
  from rotten.logical_sources
  where ($1::text[] is null or project = any($1::text[]))
    and ($2::text[] is null or environment = any($2::text[]))
    and ($3::text[] is null or cluster = any($3::text[]))
    and ($6::text[] is null or role = any($6::text[]))
), wanted as (
  -- $7's contexts as dimension ids. Each is "controller#action#job_tag" with
  -- empty parts for none (so "##" is the untagged context); the job tag,
  -- last, may hold a #. A context naming a controller, action or job tag no
  -- row has can't match anything, so it's dropped.
  select c.id as controller_id, ac.id as action_id, jt.id as job_tag_id
  from unnest($7::text[]) as w(context)
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
), counted as (
  select e.fingerprint_id, e.calls::double precision as calls, e.time::double precision as total_ms
  from rotten.fingerprints f
  join rotten.events e on e.fingerprint_id = f.id
  join sources s on s.id = e.logical_source_id
  where f.unparsed
    and not ($7::text[] is not null or $8::boolean)
    and e.observed_window_start >= $4::timestamptz
    and e.observed_window_start < $5::timestamptz
    and e.observed_window_end <= $5::timestamptz
  union all
  -- With $7 or $8, the calls and time event_context attributes to the kept
  -- contexts instead.
  select e.fingerprint_id, ec.c::double precision, coalesce(ec.attributed_time, 0)::double precision
  from rotten.fingerprints f
  join rotten.events e on e.fingerprint_id = f.id
  join sources s on s.id = e.logical_source_id
  join rotten.event_context ec on ec.event_id = e.id and ec.observed_window_start = e.observed_window_start
  where f.unparsed
    and ($7::text[] is not null or $8::boolean)
    and e.observed_window_start >= $4::timestamptz
    and e.observed_window_start < $5::timestamptz
    and e.observed_window_end <= $5::timestamptz
    and ec.observed_window_start >= $4::timestamptz
    and ec.observed_window_start < $5::timestamptz
    and ($7::text[] is null or exists (
      select 1 from wanted w
      where w.controller_id is not distinct from ec.controller_id
        and w.action_id is not distinct from ec.action_id
        and w.job_tag_id is not distinct from ec.job_tag_id))
    and not ($8::boolean and ec.controller_id is null and ec.action_id is null and ec.job_tag_id is null)
)
select
  count(distinct fingerprint_id) as fingerprints,
  coalesce(sum(calls), 0)::double precision as calls,
  coalesce(sum(total_ms), 0)::double precision as total_ms
from counted;
