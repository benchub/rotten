-- The top contexts (controller and action, or job tag) for one fingerprint,
-- for a source filter and observed time range.
--
-- Parameters:
--   $1 projects (text[]), or NULL for every project
--   $2 environments (text[]), or NULL for every environment
--   $3 clusters (text[]), or NULL for every cluster
--   $4 fingerprint_id
--   $5 window start, inclusive
--   $6 window end, exclusive for starts; windows must also end at or before it
--   $7 row limit
--   $8 roles (text[]), or NULL for every role in the sources above
--   $9 ignore untagged (boolean): leave out the untagged context's row
--
-- The untagged context, calls pg_stat_statement_context didn't attribute and
-- every call from a database without it, has controller, action and job_tag
-- all null.
--
-- times is the sum of event_context.c, so a context's calls across every
-- matching role. Ties sort by controller, action, then job tag. Only windows
-- fully inside [$5, $6) are counted; straddling windows are excluded on
-- purpose, the same as the sibling reports. Both event tables are filtered by
-- observed_window_start so the planner prunes partitions.
with sources as (
  select id
  from rotten.logical_sources
  where ($1::text[] is null or project = any($1::text[]))
    and ($2::text[] is null or environment = any($2::text[]))
    and ($3::text[] is null or cluster = any($3::text[]))
    and ($8::text[] is null or role = any($8::text[]))
), matched as (
  select e.id
  from rotten.events e
  join sources s on s.id = e.logical_source_id
  where e.fingerprint_id = $4::bigint
    and e.observed_window_start >= $5::timestamptz
    and e.observed_window_start < $6::timestamptz
    and e.observed_window_end <= $6::timestamptz
)
select
  c.controller,
  ac.action,
  jt.job_tag,
  sum(ec.c) as times
from matched m
join rotten.event_context ec on ec.event_id = m.id
left join rotten.controllers c on c.id = ec.controller_id
left join rotten.actions ac on ac.id = ec.action_id
left join rotten.job_tags jt on jt.id = ec.job_tag_id
where ec.observed_window_start >= $5::timestamptz
  and ec.observed_window_start < $6::timestamptz
  and ec.observed_window_end <= $6::timestamptz
  and not ($9::boolean and ec.controller_id is null and ec.action_id is null and ec.job_tag_id is null)
group by c.controller, ac.action, jt.job_tag
order by times desc, coalesce(c.controller, ''), coalesce(ac.action, ''), coalesce(jt.job_tag, '')
limit $7::integer;
