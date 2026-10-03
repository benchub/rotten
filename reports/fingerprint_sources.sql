-- Stats for one fingerprint on each source (role) of a project, environment
-- and cluster: its calls and time in an observed time range, plus the
-- per-call mean_time history the ingest server keeps in fingerprint_stats.
--
-- Parameters:
--   $1 project
--   $2 environment
--   $3 cluster
--   $4 fingerprint_id
--   $5 window start, inclusive
--   $6 window end, exclusive for starts; windows must also end at or before it
--   $7 role, or NULL for every role in the project, environment and cluster
--
-- One row per logical source that has windows in the range or history; a
-- source with neither is left out. calls and total_ms are 0 and
-- avg_ms_per_call is NULL for a source with history but no windows in the
-- range. The history columns are NULL for a source with no mean_time row.
-- Only windows fully inside [$5, $6) are counted; straddling windows are
-- excluded on purpose, the same as the sibling reports.
with sources as (
  select id, role
  from rotten.logical_sources
  where project = $1
    and environment = $2
    and cluster = $3
    and ($7::text is null or role = $7::text)
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
  group by e.logical_source_id
), history as (
  select fs.logical_source_id, fs.count, fs.mean, fs.deviation
  from rotten.fingerprint_stats fs
  join sources s on s.id = fs.logical_source_id
  where fs.fingerprint_id = $4::bigint
    and fs.type = 'mean_time'
)
select
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
order by s.role;
