-- Grants for the rotten schema, table by table.
--
-- migrate runs this as rotten_owner after every goose Up, in one
-- transaction. It revokes everything from the application roles and from
-- PUBLIC, then grants exactly what each role needs. There's no blanket grant,
-- so a new table stays unreachable until it's listed here.
--
-- An operator creates the roles (see docs/plan.md). This file only grants,
-- so rotten_owner doesn't need CREATEROLE. It touches only the rotten schema,
-- so pg_partman and goose keep what they have in public.
--
-- The UI's tables, users and ui_audit_log, are granted at the end.

-- Fail clearly if the operator hasn't created a role yet.
DO $$
DECLARE
    r text;
BEGIN
    FOREACH r IN ARRAY array['rotten_ingest', 'rotten_ui', 'rotten_readonly'] LOOP
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = r) THEN
            RAISE EXCEPTION 'role "%" doesn''t exist; an operator must create it before migrate runs', r
                USING HINT = 'create role ' || r || ' login;';
        END IF;
    END LOOP;
END
$$;

-- Start from nothing.
revoke all on schema rotten from public, rotten_ingest, rotten_ui, rotten_readonly;
revoke all on all tables in schema rotten from public, rotten_ingest, rotten_ui, rotten_readonly;
revoke all on all sequences in schema rotten from public, rotten_ingest, rotten_ui, rotten_readonly;
revoke all on all functions in schema rotten from public, rotten_ingest, rotten_ui, rotten_readonly;

-- Functions rotten_owner creates later don't get PUBLIC execute by default.
-- Rerunning this is a no-op. It has to be the global form: a per-schema
-- ("in schema rotten") default can only add to the global defaults, so
-- revoking there does nothing.
alter default privileges for role rotten_owner revoke execute on functions from public;

grant usage on schema rotten to rotten_ingest, rotten_ui, rotten_readonly;

-- The event tables. Everyone reads them. Only rotten_ingest writes them,
-- and it never deletes: pg_partman retention drops old partitions.
grant select on rotten.controllers, rotten.actions, rotten.job_tags,
    rotten.logical_sources, rotten.physical_sources, rotten.logical_physical_sources, rotten.fingerprints,
    rotten.events, rotten.event_context, rotten.fingerprint_stats
    to rotten_ingest, rotten_ui, rotten_readonly;
grant insert on rotten.controllers, rotten.actions, rotten.job_tags,
    rotten.logical_sources, rotten.physical_sources, rotten.logical_physical_sources, rotten.fingerprints,
    rotten.events, rotten.event_context, rotten.fingerprint_stats
    to rotten_ingest;
grant update (project) on rotten.logical_sources to rotten_ingest;
grant update (fqdn) on rotten.physical_sources to rotten_ingest;
grant update on rotten.fingerprint_stats to rotten_ingest;
grant usage on rotten.controllers_id_seq, rotten.actions_id_seq, rotten.job_tags_id_seq,
    rotten.logical_sources_id_seq, rotten.physical_sources_id_seq,
    rotten.fingerprints_id_seq, rotten.event_id_seq
    to rotten_ingest;

-- api_keys, column by column. rotten_ingest reads only what auth needs and
-- stamps last_used_at. rotten_ui creates and revokes keys but never reads
-- secret_hash back. Neither can delete a key.
grant select (id, name, secret_hash, fqdn, revoked_at) on rotten.api_keys to rotten_ingest;
grant update (last_used_at) on rotten.api_keys to rotten_ingest;
grant select (id, name, fqdn, created_at, created_by, last_used_at, revoked_at, revoked_by)
    on rotten.api_keys to rotten_ui;
grant insert (name, secret_hash, fqdn, created_by) on rotten.api_keys to rotten_ui;
grant update (revoked_at, revoked_by) on rotten.api_keys to rotten_ui;
grant usage on rotten.api_keys_id_seq to rotten_ui;

-- ingested_batches. rotten_ingest records batches and prunes old ones
-- through prune_ingested_batches(), never with a direct DELETE.
grant select, insert on rotten.ingested_batches to rotten_ingest;
grant execute on function rotten.prune_ingested_batches() to rotten_ingest;

-- users. The Rails UI owns authentication and authorization state, including
-- session_generation, which it bumps to end a user's sessions.
grant select, insert, update, delete on rotten.users to rotten_ui;
grant usage on rotten.users_id_seq to rotten_ui;

-- ui_audit_log. Append-only for the UI: it names only the columns it fills,
-- so the database stamps id and at, and it can never update or delete.
grant select on rotten.ui_audit_log to rotten_ui;
grant insert (actor_user_id, actor_email, action, target_type, target_id, details)
    on rotten.ui_audit_log to rotten_ui;
grant usage on rotten.ui_audit_log_id_seq to rotten_ui;
