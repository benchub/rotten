-- +goose Up
-- Replica utilization splits each event's time across its contexts. Storing
-- each context's logical source and share at ingest lets the reports read
-- event_context alone, instead of joining every event in the range and
-- totaling its contexts (docs/perf.md).
--
-- The backfill rewrites every existing event_context row, and the index is
-- built with a plain CREATE INDEX, so this holds locks that block ingest
-- until it's done: on 13M context rows it took about 7 minutes in testing.
-- Stop rotten-server serve first; workers keep their batches in the outbox
-- and retry. The rewritten partitions stay about twice their size until
-- retention drops them.
set local search_path to rotten, public;

alter table event_context
    add column logical_source_id integer,
    add column attributed_time double precision;
alter table event_context_partition_template
    add column logical_source_id integer,
    add column attributed_time double precision;

comment on column event_context.logical_source_id is 'The event''s logical_source_id, copied at ingest so reports can filter contexts without joining events.';
comment on column event_context.attributed_time is 'This context''s share of the event''s time: events.time * c / the sum of c over the event''s contexts.';

update event_context ec
set logical_source_id = e.logical_source_id,
    attributed_time = e.time * ec.c::double precision / t.total::double precision
from events e,
     (select event_id, observed_window_start, sum(c) as total
      from event_context
      group by event_id, observed_window_start) t
where e.id = ec.event_id
  and e.observed_window_start = ec.observed_window_start
  and t.event_id = ec.event_id
  and t.observed_window_start = ec.observed_window_start;

create index event_context_source_window on event_context (logical_source_id, observed_window_start);

-- A server from before this migration keeps inserting context rows without
-- the new columns until it's restarted. The new server calls
-- repair_context_utilization() at startup and hourly to fill them in, a batch
-- of events at a time. This index stays empty once they're repaired.
create index event_context_utilization_missing on event_context (event_id, observed_window_start)
    where attributed_time is null;

-- SECURITY DEFINER so the server can repair without holding UPDATE on
-- event_context. It only fills rows whose attributed_time is null.
-- An event's contexts are inserted in one transaction, so the totals it sees
-- are complete.
-- +goose StatementBegin
create function repair_context_utilization(max_events integer) returns bigint
language sql security definer
set search_path = pg_catalog, pg_temp
begin atomic
    with missing as (
        select distinct event_id, observed_window_start
        from rotten.event_context
        where attributed_time is null
        limit max_events
    ), totals as (
        select ec.event_id, ec.observed_window_start, sum(ec.c) as total
        from rotten.event_context ec
        join missing m on m.event_id = ec.event_id and m.observed_window_start = ec.observed_window_start
        group by ec.event_id, ec.observed_window_start
    ), repaired as (
        update rotten.event_context ec
        set logical_source_id = e.logical_source_id,
            attributed_time = e.time * ec.c::double precision / t.total::double precision
        from totals t, rotten.events e
        where ec.event_id = t.event_id
          and ec.observed_window_start = t.observed_window_start
          and ec.attributed_time is null
          and e.id = t.event_id
          and e.observed_window_start = t.observed_window_start
        returning 1
    )
    select count(*) from repaired;
end;
-- +goose StatementEnd
revoke all on function repair_context_utilization(integer) from public;

-- +goose Down
set local search_path to rotten, public;

drop function repair_context_utilization(integer);
drop index event_context_utilization_missing;
drop index event_context_source_window;
alter table event_context_partition_template
    drop column attributed_time,
    drop column logical_source_id;
alter table event_context
    drop column attributed_time,
    drop column logical_source_id;
