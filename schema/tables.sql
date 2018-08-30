
CREATE SCHEMA "rotten";

set search_path to rotten;

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
    id serial primary key,
    fingerprint text NOT NULL,
    normalized text NOT NULL
);
create unique index fingerprints_unique on fingerprints(fingerprint);


create table events (
    id serial primary key,
    fingerprint_id int not null REFERENCES fingerprints(id),
    logical_source_id int not null REFERENCES logical_sources(id),
    physical_source_id int not null REFERENCES physical_sources(id),
    observed_window tstzrange not null,
    recorded_at timestamptz not null default clock_timestamp(),
    calls float not null,
    time float not null,
    controller_id integer REFERENCES controllers(id),
    action_id integer REFERENCES actions(id)
);
create index events_logical_calls on events (logical_source_id, calls);
create index events_logical_time on events (logical_source_id, time);
create index events_observed_window on events(logical_source_id);

create type fingerprint_stats_domain as enum (
    'calls',
    'time',
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
    fingerprint_id int not null REFERENCES fingerprints(id),
    logical_source_id int not null REFERENCES logical_sources(id),
    type fingerprint_stats_domain not NULL,
    count bigint,
    mean double precision,
    deviation double precision,
    last integer,

    primary key (fingerprint_id,logical_source_id,type)
);


grant SELECT,INSERT,UPDATE on all tables in schema rotten to "rotten-client";
grant SELECT,UPDATE on all sequences in schema rotten to "rotten-client";
grant SELECT on all tables in schema rotten to "readonly";
grant SELECT,INSERT,UPDATE,DELETE on all tables in schema rotten to "readwrite";
grant SELECT,UPDATE on all sequences in schema rotten to "readwrite";

grant usage on schema rotten to readwrite,readonly,"rotten-client";

