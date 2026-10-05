-- Queries whose recent mean time per call is slower than their own source history.
--
-- Parameters:
--   $1 project
--   $2 environment
--   $3 cluster
--   $4 window start, inclusive
--   $5 window end, exclusive for starts; windows must also end at or before it
--   $6 row limit
--   $7 sigma threshold (recommended default: 3)
--   $8 minimum history count after removing in-range samples (recommended default: 30)
--   $9 zero-stddev ratio threshold (recommended default: 2)
--   $10 role, or NULL for every role in the project, environment and cluster
--   $11 match: a case-insensitive POSIX regex (~*), or NULL for every query.
--      A query matches if its normalized text does, or if it ran in the range
--      on the same source in a context whose controller#action or job tag does. It filters
--      before the row limit.
--
-- Baseline choice:
--   fingerprint_stats rows for logical_source_id = 0 are the global aggregate
--   across all sources. This report returns the adjusted global mean/deviation
--   for context, but it does not use source 0 to decide outlier status. Each
--   logical source (for example primary or replica) is compared only with its
--   own adjusted fingerprint_stats row for type = 'mean_time'.
--
-- Stored-history adjustment:
--   ingest has already merged each in-range event into fingerprint_stats as one
--   mean_time sample x = time / calls. Before scoring, this report removes the
--   in-range samples from the stored sample stddev history. Per
--   (source, fingerprint) it computes k, sum(x), and sum(x^2), then derives:
--     n' = n - k
--     mean' = (n * mean - sum(x)) / n'
--     var' = ((n - 1) * sd^2 + n * mean^2 - sum(x^2) - n' * mean'^2) / (n' - 1)
--   var' is clamped at zero to absorb floating-point noise. The global source-0
--   values returned for context use the same subtraction, but across all
--   in-range events for the fingerprint. Because fingerprint_stats is current
--   as of now, a past report range may still include later history that this
--   query cannot identify and remove. Until the legacy worker is replaced by
--   SubmitHarvest (-39), events can also be newer than fingerprint_stats:
--   legacy workers write events directly but flush fingerprint_stats only every
--   2 × ObservationInterval (about 10 minutes by default). Callers should set
--   $5 at least that far behind now, so the newest windows are not subtracted
--   from history that has not received them yet.
--
-- Outlier definition:
--   A row is an outlier when it has at least the minimum adjusted history count
--   and its recent mean milliseconds per call is more than $7 source-owned
--   sample standard deviations above the adjusted source-owned mean. If the
--   adjusted source-owned deviation is zero, the row is an outlier only when
--   recent mean is greater than $9 times the adjusted mean. Results are ordered
--   by the source-owned score before LIMIT is applied.
with sources as (
  select id, project, environment, cluster, role
  from rotten.logical_sources
  where project = $1
    and environment = $2
    and cluster = $3
    and ($10::text is null or role = $10::text)
), matched_events as (
  -- Events in the range with a context matching $11: controller#action, or
  -- job tag. Empty when $11 is NULL.
  select ec.event_id
  from rotten.event_context ec
  join sources s on s.id = ec.logical_source_id
  left join rotten.controllers c on c.id = ec.controller_id
  left join rotten.actions ac on ac.id = ec.action_id
  left join rotten.job_tags jt on jt.id = ec.job_tag_id
  where $11::text is not null
    and ec.observed_window_start >= $4::timestamptz
    and ec.observed_window_start < $5::timestamptz
    and ec.observed_window_end <= $5::timestamptz
    and (
      ((ec.controller_id is not null or ec.action_id is not null)
        and coalesce(c.controller, '') || '#' || coalesce(ac.action, '') ~* $11::text)
      or jt.job_tag ~* $11::text
    )
), aggregated as (
  select
    s.id as logical_source_id,
    s.project,
    s.environment,
    s.cluster,
    s.role,
    e.fingerprint_id,
    array_agg(e.id) as event_ids,
    count(*)::bigint as sample_count,
    sum(e.time / e.calls)::double precision as sample_sum,
    sum((e.time / e.calls) * (e.time / e.calls))::double precision as sample_sum_squares,
    sum(e.calls)::double precision as calls,
    sum(e.time)::double precision as total_ms,
    avg(e.time / e.calls)::double precision as avg_ms_per_call
  from rotten.events e
  join sources s on s.id = e.logical_source_id
  -- Only windows fully inside [start, end) are counted; straddling windows are excluded on purpose.
  where e.observed_window_start >= $4::timestamptz
    and e.observed_window_start < $5::timestamptz
    and e.observed_window_end <= $5::timestamptz
    and e.calls > 0
  group by s.id, s.project, s.environment, s.cluster, s.role, e.fingerprint_id
  -- $11 keeps a group only if one of its events ran in a matching context, or
  -- its query text matches. NULL keeps every group.
  having $11::text is null
    or bool_or(e.id in (select event_id from matched_events))
    or (select f.normalized ~* $11::text from rotten.fingerprints f where f.id = e.fingerprint_id)
), global_range_samples as (
  select
    e.fingerprint_id,
    count(*)::bigint as sample_count,
    sum(e.time / e.calls)::double precision as sample_sum,
    sum((e.time / e.calls) * (e.time / e.calls))::double precision as sample_sum_squares
  from rotten.events e
  where e.observed_window_start >= $4::timestamptz
    and e.observed_window_start < $5::timestamptz
    and e.observed_window_end <= $5::timestamptz
    and e.calls > 0
    and e.fingerprint_id in (select fingerprint_id from aggregated)
  group by e.fingerprint_id
), histories as (
  select
    a.logical_source_id,
    a.project,
    a.environment,
    a.cluster,
    a.role,
    a.fingerprint_id,
    a.event_ids,
    a.calls,
    a.total_ms,
    a.avg_ms_per_call,
    source_count.history_count as source_history_count,
    source_mean.mean_ms as source_mean_ms,
    case
      when source_count.history_count > 1 then sqrt(greatest(0::double precision, (
        ((source_stats.count - 1) * source_stats.deviation * source_stats.deviation)
        + (source_stats.count * source_stats.mean * source_stats.mean)
        - a.sample_sum_squares
        - (source_count.history_count * source_mean.mean_ms * source_mean.mean_ms)
      ) / (source_count.history_count - 1)))
      else 0::double precision
    end as source_deviation_ms,
    global_mean.mean_ms as global_mean_ms,
    case
      when global_count.history_count > 1 then sqrt(greatest(0::double precision, (
        ((global_stats.count - 1) * global_stats.deviation * global_stats.deviation)
        + (global_stats.count * global_stats.mean * global_stats.mean)
        - coalesce(gr.sample_sum_squares, 0)
        - (global_count.history_count * global_mean.mean_ms * global_mean.mean_ms)
      ) / (global_count.history_count - 1)))
      when global_count.history_count = 1 then 0::double precision
      else null::double precision
    end as global_deviation_ms
  from aggregated a
  join rotten.fingerprint_stats source_stats on source_stats.fingerprint_id = a.fingerprint_id
    and source_stats.logical_source_id = a.logical_source_id
    and source_stats.type = 'mean_time'
  left join rotten.fingerprint_stats global_stats on global_stats.fingerprint_id = a.fingerprint_id
    and global_stats.logical_source_id = 0
    and global_stats.type = 'mean_time'
  left join global_range_samples gr on gr.fingerprint_id = a.fingerprint_id
  cross join lateral (
    select (source_stats.count - a.sample_count)::double precision as history_count
  ) source_count
  cross join lateral (
    select ((source_stats.count * source_stats.mean) - a.sample_sum) / nullif(source_count.history_count, 0) as mean_ms
  ) source_mean
  cross join lateral (
    select
      case
        when global_stats.count is null then null::double precision
        else (global_stats.count - coalesce(gr.sample_count, 0))::double precision
      end as history_count
  ) global_count
  cross join lateral (
    select
      case
        when global_count.history_count is null then null::double precision
        else ((global_stats.count * global_stats.mean) - coalesce(gr.sample_sum, 0)) / nullif(global_count.history_count, 0)
      end as mean_ms
  ) global_mean
), scored as (
  select
    h.logical_source_id,
    h.project,
    h.environment,
    h.cluster,
    h.role,
    h.fingerprint_id,
    h.event_ids,
    h.calls,
    h.total_ms,
    h.avg_ms_per_call,
    h.global_mean_ms,
    h.global_deviation_ms,
    h.source_mean_ms,
    h.source_deviation_ms,
    h.source_mean_ms as baseline_mean_ms,
    h.source_deviation_ms as baseline_deviation_ms,
    case
      when h.source_deviation_ms > 0 then ((h.avg_ms_per_call - h.source_mean_ms) / h.source_deviation_ms)::double precision
      when h.source_mean_ms > 0 then (h.avg_ms_per_call / h.source_mean_ms)::double precision
      when h.avg_ms_per_call > 0 then 'Infinity'::double precision
      else 0::double precision
    end as deviations_over_source
  from histories h
  where h.source_history_count >= $8::integer
    and (
      (h.source_deviation_ms > 0 and h.avg_ms_per_call > h.source_mean_ms + ($7::double precision * h.source_deviation_ms))
      or (h.source_deviation_ms = 0 and h.avg_ms_per_call > $9::double precision * h.source_mean_ms)
    )
), limited as (
  select *
  from scored
  order by deviations_over_source desc, avg_ms_per_call desc, total_ms desc, fingerprint_id, logical_source_id
  limit $6::integer
), contexts as (
  select
    l.logical_source_id,
    l.fingerprint_id,
    sum(ec.c) as times,
    c.controller,
    ac.action,
    jt.job_tag,
    row_number() over (
      partition by l.logical_source_id, l.fingerprint_id
      order by sum(ec.c) desc, coalesce(c.controller, ''), coalesce(ac.action, ''), coalesce(jt.job_tag, '')
    ) as r
  from limited l
  join rotten.event_context ec on ec.event_id = any(l.event_ids)
  left join rotten.controllers c on c.id = ec.controller_id
  left join rotten.actions ac on ac.id = ec.action_id
  left join rotten.job_tags jt on jt.id = ec.job_tag_id
  where ec.observed_window_start >= $4::timestamptz
    and ec.observed_window_start < $5::timestamptz
    and ec.observed_window_end <= $5::timestamptz
  group by l.logical_source_id, l.fingerprint_id, c.controller, ac.action, jt.job_tag
), context_agg as (
  select
    logical_source_id,
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
  group by logical_source_id, fingerprint_id
)
select
  l.logical_source_id,
  l.fingerprint_id,
  l.project,
  l.environment,
  l.cluster,
  l.role,
  l.calls,
  l.total_ms,
  l.avg_ms_per_call,
  l.global_mean_ms,
  l.global_deviation_ms,
  l.source_mean_ms,
  l.source_deviation_ms,
  l.baseline_mean_ms,
  l.baseline_deviation_ms,
  l.deviations_over_source,
  left(regexp_replace(f.normalized, '\n', ' ', 'g'), 250) as example,
  coalesce(ca.contexts, '[]'::jsonb) as context
from limited l
join rotten.fingerprints f on f.id = l.fingerprint_id
left join context_agg ca on ca.logical_source_id = l.logical_source_id
  and ca.fingerprint_id = l.fingerprint_id
order by l.deviations_over_source desc, l.avg_ms_per_call desc, l.total_ms desc, l.fingerprint_id, l.logical_source_id;
