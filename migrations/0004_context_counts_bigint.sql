-- +goose Up
-- This rewrites rotten.event_context and each existing partition under
-- ACCESS EXCLUSIVE locks. Schedule it for a maintenance window.
set local search_path to rotten, public;

alter table event_context alter column c type bigint;
alter table event_context_partition_template alter column c type bigint;

-- +goose Down
-- This fails if any event_context.c value is above integer range.
set local search_path to rotten, public;

alter table event_context alter column c type integer;
alter table event_context_partition_template alter column c type integer;
