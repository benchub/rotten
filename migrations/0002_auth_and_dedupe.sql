-- +goose Up
-- Pass keys for workers, and the batch IDs the server has already ingested.
-- Grants live in permissions.sql, which migrate reapplies after every run.
set local search_path to rotten, public;

create table api_keys (
    id bigserial primary key,
    name text not null,
    -- A hash of the secret. The secret itself is shown once and never stored.
    secret_hash text not null,
    -- If set, only a worker on this host may use the key.
    fqdn text,
    created_at timestamptz not null default now(),
    created_by text not null,
    last_used_at timestamptz,
    revoked_at timestamptz,
    revoked_by text,
    check ((revoked_at is null) = (revoked_by is null))
);
create unique index api_keys_secret_hash on api_keys (secret_hash);
create unique index api_keys_name on api_keys (name);

-- A worker retries a batch until the server acks it, so the server records
-- each batch_id and skips repeats. Rows older than 30 days get pruned by
-- prune_ingested_batches(), which the server calls.
create table ingested_batches (
    batch_id text primary key,
    key_id bigint not null references api_keys(id),
    received_at timestamptz not null default now()
);
create index ingested_batches_received_at on ingested_batches (received_at);

-- SECURITY DEFINER so the server can prune without holding DELETE on the
-- table. It runs as rotten_owner and only ever deletes rows older than 30
-- days. permissions.sql grants EXECUTE to rotten_ingest only.
-- +goose StatementBegin
create function prune_ingested_batches() returns bigint
language sql security definer
set search_path = pg_catalog, pg_temp
begin atomic
    with d as (
        delete from rotten.ingested_batches
        where received_at < now() - interval '30 days'
        returning 1
    )
    select count(*) from d;
end;
-- +goose StatementEnd
revoke all on function prune_ingested_batches() from public;

-- +goose Down
drop function rotten.prune_ingested_batches();
drop table rotten.ingested_batches;
drop table rotten.api_keys;
