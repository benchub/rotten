-- +goose Up
-- The landing page's stats and heat map. They read a week of events, so the
-- UI doesn't compute them per request: rotten-server calls
-- refresh_home_stats() at startup and every 5 minutes, and the UI reads these
-- two small tables. The refresh replaces both tables' rows in one
-- transaction, so readers see the old numbers until it commits, as with
-- REFRESH MATERIALIZED VIEW CONCURRENTLY. Unlike a materialized view, it
-- takes the time it covers as a parameter, so tests and the perf suite can
-- refresh fixed data. See docs/ui.md ("Landing page") for the definitions.
set local search_path to rotten, public;

-- One row. as_of is the end of the time the numbers cover; computed_at is
-- when the refresh ran.
create table home_stats (
    singleton boolean primary key default true check (singleton),
    computed_at timestamptz not null,
    as_of timestamptz not null,
    events_last_hour bigint not null,
    fingerprints bigint not null,
    hosts_last_hour integer not null,
    hosts_last_week integer not null
);

comment on column home_stats.events_last_hour is 'Events (one per fingerprint, logical and physical source, and harvest window) whose window starts in the hour before as_of.';
comment on column home_stats.fingerprints is 'Rows in rotten.fingerprints: every distinct query rotten has stored.';
comment on column home_stats.hosts_last_hour is 'Physical sources (observed database hosts, by fqdn) with an event whose window starts in the hour before as_of.';
comment on column home_stats.hosts_last_week is 'Physical sources with an event whose window starts in the 7 days before as_of.';

-- One row per hour and duration bucket with any fingerprints in it.
create table home_heatmap (
    hour timestamptz not null,
    duration_bucket smallint not null,
    fingerprints integer not null,
    primary key (hour, duration_bucket)
);

comment on column home_heatmap.hour is 'The hour the events'' windows start in, in UTC.';
comment on column home_heatmap.duration_bucket is 'floor(2 * log10(mean ms per call)), clamped to -4 (under 0.0316 ms) through 10 (100 s and up): bucket b holds [10^(b/2), 10^((b+1)/2)) ms.';
comment on column home_heatmap.fingerprints is 'Distinct fingerprints with an event in this hour whose mean time per call (time / calls) falls in this bucket.';

-- SECURITY DEFINER so the server can refresh without holding write
-- privileges on the tables. An advisory lock keeps two servers from
-- refreshing at once: the one that doesn't get it returns false at once.
-- It trusts as_of from its caller, rotten_ingest (the server, which passes
-- now()): a caller could set the window the numbers cover, but not write
-- anything else into the tables.
-- +goose StatementBegin
create function refresh_home_stats(as_of timestamptz) returns boolean
language plpgsql security definer
set search_path = pg_catalog, pg_temp
as $$
begin
    if not pg_try_advisory_xact_lock(hashtext('rotten.refresh_home_stats')) then
        return false;
    end if;

    delete from rotten.home_stats;
    insert into rotten.home_stats (computed_at, as_of, events_last_hour, fingerprints, hosts_last_hour, hosts_last_week)
    select clock_timestamp(), as_of,
        count(*) filter (where e.observed_window_start >= as_of - interval '1 hour'),
        (select count(*) from rotten.fingerprints),
        count(distinct e.physical_source_id) filter (where e.observed_window_start >= as_of - interval '1 hour'),
        count(distinct e.physical_source_id)
    from rotten.events e
    where e.observed_window_start >= as_of - interval '7 days'
      and e.observed_window_start < as_of;

    delete from rotten.home_heatmap;
    insert into rotten.home_heatmap (hour, duration_bucket, fingerprints)
    select date_trunc('hour', e.observed_window_start, 'UTC'),
        greatest(-4, least(10, floor(2 * log(greatest(e.time / e.calls, 1e-9)))))::smallint,
        count(distinct e.fingerprint_id)
    from rotten.events e
    where e.observed_window_start >= as_of - interval '7 days'
      and e.observed_window_start < as_of
      and e.calls > 0
    group by 1, 2;

    return true;
end;
$$;
-- +goose StatementEnd
revoke all on function refresh_home_stats(timestamptz) from public;

-- +goose Down
set local search_path to rotten, public;

drop function refresh_home_stats(timestamptz);
drop table home_heatmap;
drop table home_stats;
