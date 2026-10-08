-- The top contexts (controller and action, or job tag) for one fingerprint,
-- for a source filter and observed time range.
--
-- Parameters:
--   $1 project
--   $2 environment
--   $3 cluster
--   $4 fingerprint_id
--   $5 window start, inclusive
--   $6 window end, exclusive for starts; windows must also end at or before it
--   $7 row limit
--   $8 role, or NULL for every role in the project, environment and cluster
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
  where project = $1
    and environment = $2
    and cluster = $3
    and ($8::text is null or role = $8::text)
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
group by c.controller, ac.action, jt.job_tag
order by times desc, coalesce(c.controller, ''), coalesce(ac.action, ''), coalesce(jt.job_tag, '')
limit $7::integer;
