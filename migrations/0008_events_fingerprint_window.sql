-- +goose Up
-- The per-fingerprint reports (fingerprint_timeseries, fingerprint_contexts
-- and fingerprint_sources) look up one fingerprint over days. Without this
-- index they read every page of the source's partitions in the range; see
-- docs/perf.md for the numbers.
--
-- Creating an index on the partitioned parent builds it on every existing
-- partition in this transaction, holding a SHARE lock that blocks inserts
-- into rotten.events until it's done. Workers keep their batches in the
-- outbox and retry, but schedule it for a quiet time. New partitions get the
-- index automatically. Each insert now maintains one more btree on events.
set local search_path to rotten, public;

create index events_fingerprint_window on events (fingerprint_id, observed_window_start);

-- +goose Down
set local search_path to rotten, public;

drop index events_fingerprint_window;
