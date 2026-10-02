-- +goose Up
-- Link physical sources to every logical source they serve, and retain
-- enough batch metadata to enforce physical-source window idempotence and
-- overlap checks.
set local search_path to rotten, public;

create table logical_physical_sources (
    logical_source_id int not null references logical_sources(id),
    physical_source_id int not null references physical_sources(id),
    primary key (logical_source_id, physical_source_id)
);

alter table ingested_batches add column logical_source_id int references logical_sources(id);
alter table ingested_batches add column physical_source_id int references physical_sources(id);
alter table ingested_batches add column observed_window_start timestamptz;
alter table ingested_batches add column observed_window_end timestamptz;
alter table ingested_batches add column content_hash bytea;
alter table ingested_batches add constraint ingested_batches_window_order
    check (observed_window_end is null or observed_window_start is null or observed_window_end > observed_window_start);
create index ingested_batches_physical_window on ingested_batches
    (physical_source_id, observed_window_start, observed_window_end)
    where physical_source_id is not null;

-- +goose Down
drop index rotten.ingested_batches_physical_window;
alter table ingested_batches drop constraint ingested_batches_window_order;
alter table ingested_batches drop column content_hash;
alter table ingested_batches drop column observed_window_end;
alter table ingested_batches drop column observed_window_start;
alter table ingested_batches drop column physical_source_id;
alter table ingested_batches drop column logical_source_id;
drop table logical_physical_sources;
