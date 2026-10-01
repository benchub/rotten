
-- +goose Up
-- The baseline schema. migrate runs this as rotten_owner, which owns the
-- database, so rotten_owner owns the schema and everything in it. Grants for
-- the other roles live in a later migration.
CREATE SCHEMA "rotten";

-- goose runs each migration in one transaction on one connection, so this
-- holds for the rest of the file. public stays on the path for
-- pg_partman's functions.
set local search_path to rotten, public;

create table controllers (
    id serial primary key,
    controller text not null
);
create unique index controllers_unique on controllers (controller);

create table actions (
    id serial primary key,
    action text not null
);
create unique index actions_unique on actions (action);

create table job_tags (
    id serial primary key,
    job_tag text not null
);
create unique index job_tags_unique on job_tags (job_tag);

create table logical_sources (
    id serial primary key,
    project text not null,
    environment text not null,
    cluster text not null,
    role text not null
);
create unique index logical_sources_unique on logical_sources (cluster,role,project,environment);

-- this special logical source is for everything
insert into logical_sources (id,project,environment,cluster,role) values (0,'all','all','all','all');

create table physical_sources (
    id serial primary key,
    fqdn text not null
);
create unique index physical_sources_unique on physical_sources (fqdn);

CREATE TABLE fingerprints (
    id bigserial primary key,
    fingerprint text NOT NULL,
    normalized text NOT NULL
);
create unique index fingerprints_unique on fingerprints(fingerprint);

comment on column fingerprints.fingerprint is 'The query as pg_stat_statements saw it, after normalizing schema qualifiers and variable-length IN and VALUES clauses.';
comment on column fingerprints.normalized is 'The fingerprint with placeholders for values.';


create sequence event_id_seq owned by none;
create table events (
    id bigint not null default nextval('rotten.event_id_seq'),
    fingerprint_id bigint not null REFERENCES fingerprints(id),
    logical_source_id int not null REFERENCES logical_sources(id),
    physical_source_id int not null REFERENCES physical_sources(id),
    observed_window_start timestamptz not null,
    observed_window_end timestamptz not null check (observed_window_end > observed_window_start),
    recorded_at timestamptz not null default clock_timestamp(),
    calls float not null,
    time float not null
) partition by range (observed_window_start);
create index events_observed_window_start on events (observed_window_start);
create index events_observed_window_end on events (observed_window_end);
create index events_logical_calls on events (logical_source_id, calls);
create index events_logical_time on events (logical_source_id, time);

create table events_partition_template (like events);
alter table events_partition_template add primary key (id);


SELECT public.create_parent('rotten.events', 'observed_window_start', '1 day', p_template_table := 'rotten.events_partition_template');
update public.part_config set retention='21 days',retention_keep_table=false where parent_table='rotten.events';


create table event_context (
    event_id bigint not null,
    observed_window_start timestamptz not null,
    observed_window_end timestamptz not null check (observed_window_end > observed_window_start),
    controller_id integer REFERENCES controllers(id),
    action_id integer REFERENCES actions(id),
    job_tag_id integer REFERENCES job_tags(id),
    c integer not null check(c > 0)
) partition by range (observed_window_start);
create index event_context_event on event_context (event_id);

create table event_context_partition_template (like event_context);

SELECT public.create_parent('rotten.event_context', 'observed_window_start', '1 day', p_template_table := 'rotten.event_context_partition_template');
update public.part_config set retention='21 days',retention_keep_table=false where parent_table='rotten.event_context';


create type fingerprint_stats_domain as enum (
    'calls',
    'total_time',
    'min_time',
    'max_time',
    'mean_time',
    'stddev_time',
    'rows',
    'shared_blks_hit',
    'shared_blks_read',
    'shared_blks_dirtied',
    'shared_blks_written',
    'local_blks_hit',
    'local_blks_read',
    'local_blks_dirtied',
    'local_blks_written',
    'temp_blks_read',
    'temp_blks_written',
    'blk_read_time',
    'blk_write_time'
);

CREATE TABLE fingerprint_stats (
    fingerprint_id bigint not null REFERENCES fingerprints(id),
    logical_source_id int not null REFERENCES logical_sources(id),
    type fingerprint_stats_domain not NULL,
    count bigint,
    mean double precision,
    deviation double precision,
    last bigint,

    primary key (fingerprint_id,logical_source_id,type)
);


-- New sessions find rotten's tables without schema-qualifying them.
-- +goose StatementBegin
DO $$
BEGIN
    EXECUTE format('alter database %I set search_path to "$user", rotten, public', current_database());
END
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    EXECUTE format('alter database %I reset search_path', current_database());
END
$$;
-- +goose StatementEnd
delete from public.part_config where parent_table in ('rotten.events', 'rotten.event_context');
drop schema rotten cascade;
