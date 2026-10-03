-- Calls and total time for one fingerprint over time, for a source filter.
--
-- Parameters:
--   $1 project
--   $2 environment
--   $3 cluster
--   $4 fingerprint_id
--   $5 window start, inclusive
--   $6 window end, exclusive for starts; windows must also end at or before it
--   $7 bucket width interval
--
-- Source filter:
--   Source means the same project/environment/cluster filter as the sibling
--   reports. Role is deliberately aggregated, so primary and replica rows for
--   that source filter contribute to the same bucket.
--
-- Buckets:
--   The bucket width is an interval, but it must not contain month or year
--   parts because those are calendar-relative instead of fixed durations.
--   The report normalizes the width to fixed seconds once, so 1 day means 24
--   hours even across daylight-saving transitions. Buckets are generated with
--   generate_series for chart-friendly output, so empty buckets are returned
--   with zero calls and zero total_ms. Bucket starts are aligned to $5, and
--   events are assigned with date_bin(width, observed_window_start, $5). A
--   window that straddles a bucket boundary is assigned to the bucket
--   containing its start. The final trailing bucket is returned even when it
--   is shorter than the requested width.
--
-- Limits:
--   The report rejects requests that would generate more than 10000 buckets.
--   The index decision for (fingerprint_id, observed_window_start) is deferred
--   to the -53 scale test, because the small report fixture is not enough
--   evidence to justify another partitioned-table index.
--
-- Window semantics:
--   Only windows fully inside [$5, $6) are counted: observed_window_start must
--   be at or after $5 and before $6, and observed_window_end must be at or
--   before $6. Straddling the report range is excluded on purpose. Empty or
--   inverted ranges ($6 <= $5) return an empty result.
with input as (
  select
    $5::timestamptz as start_at,
    $6::timestamptz as end_at,
    make_interval(secs => extract(epoch from $7::interval)) as width,
    case
      when extract(year from $7::interval) <> 0
        or extract(month from $7::interval) <> 0
        then ('fingerprint_timeseries bucket width must not contain months or years: ' || $7::text)::integer
      when extract(epoch from $7::interval) <= 0
        then ('fingerprint_timeseries bucket width must be positive: ' || $7::text)::integer
      when $6::timestamptz <= $5::timestamptz then 1
      when ceil(extract(epoch from ($6::timestamptz - $5::timestamptz)) / extract(epoch from $7::interval)) > 10000
        then ('fingerprint_timeseries bucket count must be at most 10000: ' || $7::text)::integer
      else 1
    end as ok
), sources as (
  select id
  from rotten.logical_sources
  where project = $1
    and environment = $2
    and cluster = $3
), buckets as (
  select
    gs.bucket_start,
    least(gs.bucket_start + i.width, i.end_at) as bucket_end
  from input i
  cross join lateral generate_series(
    i.start_at,
    i.end_at - interval '1 microsecond',
    i.width
  ) as gs(bucket_start)
  where i.ok = 1
    and i.end_at > i.start_at
), aggregated as (
  select
    date_bin(i.width, e.observed_window_start, i.start_at) as bucket_start,
    sum(e.calls)::bigint as calls,
    sum(e.time)::double precision as total_ms
  from rotten.events e
  cross join input i
  join sources s on s.id = e.logical_source_id
  where e.fingerprint_id = $4::bigint
    and i.ok = 1
    and e.observed_window_start >= $5::timestamptz
    and e.observed_window_start < $6::timestamptz
    and e.observed_window_end <= $6::timestamptz
  group by date_bin(i.width, e.observed_window_start, i.start_at)
)
select
  b.bucket_start,
  b.bucket_end,
  coalesce(a.calls, 0)::bigint as calls,
  coalesce(a.total_ms, 0)::double precision as total_ms
from buckets b
left join aggregated a on a.bucket_start = b.bucket_start
order by b.bucket_start;
