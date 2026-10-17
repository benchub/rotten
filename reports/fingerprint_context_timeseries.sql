-- Calls and time for one fingerprint in some contexts over time, for a source
-- filter. The same buckets as fingerprint_timeseries.sql, counting only those
-- contexts' share.
--
-- Parameters:
--   $1 projects (text[]), or NULL for every project
--   $2 environments (text[]), or NULL for every environment
--   $3 clusters (text[]), or NULL for every cluster
--   $4 fingerprint_id
--   $5 window start, inclusive
--   $6 window end, exclusive for starts; windows must also end at or before it
--   $7 bucket width interval
--   $8 roles (text[]), or NULL for every role in the sources above
--   $9 contexts (text[]): "controller#action#job_tag", empty parts for none
--      (so "##" is the untagged context), or NULL for every context
--   $10 ignore untagged (boolean): leave out the untagged context
--
-- Context:
--   A context is the (controller, action, job tag) triple the contexts
--   report groups by, and it matches only that exact triple: a NULL part
--   matches only rows where that part is null. All three NULL is the
--   untagged context. A context the fingerprint never ran in gives all-zero
--   buckets rather than an error.
--
-- Calls and time:
--   calls is the sum of event_context.c. total_ms is the sum of
--   event_context.attributed_time, an estimate of the context's share of its
--   event's time (see the column comment, migrations 0011 and 0015): the
--   shares of one event add up to its time, but the context's own time isn't
--   measured exactly. Rows ingested before the repair has filled
--   attributed_time count as 0 ms.
--
-- Buckets, source filter, limits and window semantics are those of
-- fingerprint_timeseries.sql. Events come from events_source_fingerprint_window
-- (migration 0013), then their event_context rows by event_id, with
-- observed_window_start filters on both tables so the planner prunes
-- partitions.
with input as (
  select
    $5::timestamptz as start_at,
    $6::timestamptz as end_at,
    make_interval(secs => extract(epoch from $7::interval)) as width,
    case
      when extract(year from $7::interval) <> 0
        or extract(month from $7::interval) <> 0
        then ('fingerprint_context_timeseries bucket width must not contain months or years: ' || $7::text)::integer
      when extract(epoch from $7::interval) <= 0
        then ('fingerprint_context_timeseries bucket width must be positive: ' || $7::text)::integer
      when $6::timestamptz <= $5::timestamptz then 1
      when ceil(extract(epoch from ($6::timestamptz - $5::timestamptz)) / extract(epoch from $7::interval)) > 10000
        then ('fingerprint_context_timeseries bucket count must be at most 10000: ' || $7::text)::integer
      else 1
    end as ok
), sources as (
  select id
  from rotten.logical_sources
  where ($1::text[] is null or project = any($1::text[]))
    and ($2::text[] is null or environment = any($2::text[]))
    and ($3::text[] is null or cluster = any($3::text[]))
    and ($8::text[] is null or role = any($8::text[]))
), wanted as (
  -- $9's contexts as dimension ids. Each is "controller#action#job_tag" with
  -- empty parts for none (so "##" is the untagged context); the job tag,
  -- last, may hold a #. A context naming a controller, action or job tag no
  -- row has can't match anything, so it's dropped.
  select c.id as controller_id, ac.id as action_id, jt.id as job_tag_id
  from unnest($9::text[]) as w(context)
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
), matched as (
  select e.id
  from rotten.events e
  join sources s on s.id = e.logical_source_id
  where e.fingerprint_id = $4::bigint
    and e.observed_window_start >= $5::timestamptz
    and e.observed_window_start < $6::timestamptz
    and e.observed_window_end <= $6::timestamptz
), aggregated as (
  select
    date_bin(i.width, ec.observed_window_start, i.start_at) as bucket_start,
    sum(ec.c)::bigint as calls,
    sum(coalesce(ec.attributed_time, 0))::double precision as total_ms
  from matched m
  cross join input i
  join rotten.event_context ec on ec.event_id = m.id
  where i.ok = 1
    and ec.observed_window_start >= $5::timestamptz
    and ec.observed_window_start < $6::timestamptz
    and ec.observed_window_end <= $6::timestamptz
    and ($9::text[] is null or exists (
      select 1 from wanted w
      where w.controller_id is not distinct from ec.controller_id
        and w.action_id is not distinct from ec.action_id
        and w.job_tag_id is not distinct from ec.job_tag_id))
    and not ($10::boolean and ec.controller_id is null and ec.action_id is null and ec.job_tag_id is null)
  group by date_bin(i.width, ec.observed_window_start, i.start_at)
)
select
  b.bucket_start,
  b.bucket_end,
  coalesce(a.calls, 0)::bigint as calls,
  coalesce(a.total_ms, 0)::double precision as total_ms
from buckets b
left join aggregated a on a.bucket_start = b.bucket_start
order by b.bucket_start;
