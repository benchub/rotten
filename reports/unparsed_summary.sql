-- How many fingerprints in the range are fallbacks, hashed from the query
-- text because the worker's parser rejected the statement, and their calls
-- and time. The UI shows it under the top and outlier reports.
--
-- Parameters, as for the top reports:
--   $1 project
--   $2 environment
--   $3 cluster
--   $4 window start, inclusive
--   $5 window end, exclusive for starts; windows must also end at or before it
--   $6 role, or NULL for every role in the project, environment and cluster
--
-- Unparsed fingerprints are rare, so this starts from them (the partial
-- index fingerprints_unparsed) and reads their events by
-- events_fingerprint_window, rather than scanning the range.
with sources as (
  select id
  from rotten.logical_sources
  where project = $1
    and environment = $2
    and cluster = $3
    and ($6::text is null or role = $6::text)
)
select
  count(distinct e.fingerprint_id) as fingerprints,
  coalesce(sum(e.calls), 0)::double precision as calls,
  coalesce(sum(e.time), 0)::double precision as total_ms
from rotten.fingerprints f
join rotten.events e on e.fingerprint_id = f.id
join sources s on s.id = e.logical_source_id
where f.unparsed
  and e.observed_window_start >= $4::timestamptz
  and e.observed_window_start < $5::timestamptz
  and e.observed_window_end <= $5::timestamptz;
