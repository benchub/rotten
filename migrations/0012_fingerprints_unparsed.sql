-- +goose Up
-- Statements the worker's pinned parser rejects (some Postgres 18 syntax) are
-- sent under a fallback fingerprint hashed from their text, marked unparsed
-- (docs/worker.md). Adding a column with a constant default doesn't rewrite
-- the table.
set local search_path to rotten, public;

alter table fingerprints add column unparsed boolean not null default false;

-- A worker that sent fallback fingerprints to a server from before this
-- migration got them stored as plain ones. No pg_query fingerprint has the
-- prefix, so it marks them. ('_' would be a LIKE wildcard; there's none.)
update fingerprints set unparsed = true where fingerprint like 'unparsed-%';

-- reports/unparsed_summary.sql starts from the unparsed fingerprints, which
-- should be few. Built in this transaction; it reads fingerprints once.
create index fingerprints_unparsed on fingerprints (id) where unparsed;

comment on column fingerprints.fingerprint is 'Hex string from fingerprint.Normalized, or unparsed- plus a hash of the query text when the parser rejected it (see unparsed); matches proto.rotten.v1.FingerprintAggregate.fingerprint.';
comment on column fingerprints.normalized is 'pg_query.Normalize output of one representative query text for this fingerprint, or the pg_stat_statements text with leading and trailing comments stripped when unparsed; stored only on first insert.';
comment on column fingerprints.unparsed is 'True when the worker''s parser rejected the statement, so fingerprint is a hash of its text rather than a pg_query fingerprint. Set on first insert, or later for an unparsed- fingerprint an older server stored; never cleared. False for workers older than the flag.';

-- +goose Down
set local search_path to rotten, public;

drop index fingerprints_unparsed;
alter table fingerprints drop column unparsed;

comment on column fingerprints.fingerprint is 'Hex string from fingerprint.Normalized, matching proto.rotten.v1.FingerprintAggregate.fingerprint.';
comment on column fingerprints.normalized is 'pg_query.Normalize output of one representative query text for this fingerprint, stored only on first insert.';
