-- +goose Up
-- What admins do in the UI, such as creating and revoking pass keys. The UI
-- writes a row in the same transaction as the change it records. rotten_ui
-- may only insert and read (see permissions.sql), and the database stamps id
-- and at, so the UI can't rewrite or backdate history. There's no foreign
-- key to users or api_keys: the log outlives the rows it mentions, and
-- actor_email keeps the actor readable after their user row is gone.
set local search_path to rotten, public;

create table ui_audit_log (
    id bigserial primary key,
    at timestamptz not null default now(),
    actor_user_id bigint,
    actor_email text not null,
    action text not null check (action ~ '^[a-z][a-z_]*\.[a-z][a-z_]*$'),
    target_type text,
    target_id bigint,
    details jsonb not null default '{}' check (jsonb_typeof(details) = 'object')
);

create index ui_audit_log_target on ui_audit_log (target_type, target_id);

comment on table ui_audit_log is 'Append-only log of admin actions taken in the UI.';
comment on column ui_audit_log.id is 'Primary key, in insert order.';
comment on column ui_audit_log.at is 'When the action happened; set by the database.';
comment on column ui_audit_log.actor_user_id is 'users.id of the admin who acted, kept even if that user is deleted.';
comment on column ui_audit_log.actor_email is 'Email of the admin who acted, at the time.';
comment on column ui_audit_log.action is 'What happened, as <target>.<verb>, such as api_key.create or api_key.revoke.';
comment on column ui_audit_log.target_type is 'Kind of row acted on, such as api_key.';
comment on column ui_audit_log.target_id is 'Id of the row acted on, such as api_keys.id.';
comment on column ui_audit_log.details is 'Extra facts about the action as a JSON object, such as the key name and fqdn. Never a secret.';

-- +goose Down
set local search_path to rotten, public;

drop table ui_audit_log;
