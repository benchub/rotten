-- +goose Up
-- Migration 0008's events_fingerprint_window, on (fingerprint_id,
-- observed_window_start), is no longer needed: every report that reads
-- events by fingerprint also filters by logical source, or (like
-- fingerprint_all_sources) is as fast with migration 0013's
-- events_source_fingerprint_window, which leads with the source, through
-- Postgres 18's btree skip scan. Dropping it saves an index entry per event
-- at ingest and about 40% of the events heap's size on disk. See
-- docs/perf.md for the numbers.
--
-- Dropping an index on the partitioned parent drops it from every
-- partition in this transaction, holding an ACCESS EXCLUSIVE lock on
-- rotten.events and all its partitions: reports and ingest both wait, but
-- only briefly, since nothing is rebuilt (DROP INDEX CONCURRENTLY isn't
-- supported on a partitioned index). It waits first for running reports to
-- finish, and everything queued behind it waits too.
set local search_path to rotten, public;

drop index events_fingerprint_window;

-- +goose Down
-- Builds the index again on every partition, like migration 0008: a SHARE
-- lock on rotten.events that blocks inserts until it's done.
set local search_path to rotten, public;

create index events_fingerprint_window on events (fingerprint_id, observed_window_start);
