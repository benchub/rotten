-- Stats for one fingerprint across every logical source, for the all-sources
-- row under fingerprint_sources.sql on the fingerprint page. It has the same
-- columns but role, over the same time range, so the two compare directly:
-- calls and time summed over every project, environment, cluster and role,
-- plus the per-call mean_time history the ingest server keeps for all sources
-- together in fingerprint_stats under logical source 0.
--
-- Parameters:
--   $1 fingerprint_id
--   $2 window start, inclusive
--   $3 window end, exclusive for starts; windows must also end at or before it
--
-- One row, or none when the fingerprint has neither windows in the range nor
-- history. calls and total_ms are 0 and avg_ms_per_call is NULL with history
-- but no windows in the range. The history columns are NULL with no
-- mean_time row. Only windows fully inside [$2, $3) are counted; straddling
-- windows are excluded on purpose, the same as the sibling reports.
with in_range as (
  select
    count(*) as windows,
    sum(e.calls)::double precision as calls,
    sum(e.time)::double precision as total_ms
  from rotten.events e
  where e.fingerprint_id = $1::bigint
    and e.observed_window_start >= $2::timestamptz
    and e.observed_window_start < $3::timestamptz
    and e.observed_window_end <= $3::timestamptz
), history as (
  select fs.count, fs.mean, fs.deviation
  from rotten.fingerprint_stats fs
  where fs.fingerprint_id = $1::bigint
    and fs.logical_source_id = 0
    and fs.type = 'mean_time'
)
select
  coalesce(r.calls, 0)::double precision as calls,
  coalesce(r.total_ms, 0)::double precision as total_ms,
  (r.total_ms / nullif(r.calls, 0))::double precision as avg_ms_per_call,
  h.count as history_samples,
  h.mean as history_mean_ms,
  h.deviation as history_deviation_ms
from in_range r
left join history h on true
where r.windows > 0
   or exists (select from history);
