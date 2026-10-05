-- +goose Up
-- The outliers report's adaptive lookback: a (source, fingerprint) with too
-- few samples before a short range takes its newest older windows, up to 7
-- days back. This index lets it read just those windows, newest first, with
-- an index-only scan per group, instead of every row the sources have in
-- those days. It includes every other column that read needs. See
-- docs/perf.md for the numbers.
--
-- Like 0008, creating an index on the partitioned parent builds it on every
-- existing partition in this transaction, holding a SHARE lock that blocks
-- inserts into rotten.events until it's done (CONCURRENTLY isn't supported
-- on a partitioned table). Workers keep their batches in the outbox and
-- retry, but schedule it for a quiet time. New partitions get the index
-- automatically. Each insert now maintains one more btree on events.
set local search_path to rotten, public;

create index events_source_fingerprint_window on events
    (logical_source_id, fingerprint_id, observed_window_start)
    include (observed_window_end, calls, time);

-- +goose Down
set local search_path to rotten, public;

drop index events_source_fingerprint_window;
