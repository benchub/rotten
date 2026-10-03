-- +goose Up
set local search_path to rotten, public;

comment on column fingerprints.fingerprint is 'Hex string from fingerprint.Normalized, matching proto.rotten.v1.FingerprintAggregate.fingerprint.';
comment on column fingerprints.normalized is 'pg_query.Normalize output of one representative query text for this fingerprint, stored only on first insert.';

-- +goose Down
set local search_path to rotten, public;

comment on column fingerprints.fingerprint is 'The query as pg_stat_statements saw it, after normalizing schema qualifiers and variable-length IN and VALUES clauses.';
comment on column fingerprints.normalized is 'The fingerprint with placeholders for values.';
