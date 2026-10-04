-- +goose Up
-- The UI's sessions live in an encrypted cookie, so the server can't delete
-- one. Instead each session stores the user's session_generation from when it
-- started, and the UI refuses it once the column has moved on. Bumping it
-- (logout, a password change or reset, users:disable, an OIDC login refused
-- for lost group access) ends every session the user has. rotten_ui's
-- table-level update grant on users (permissions.sql) covers the new column.
-- Adding a column with a constant default doesn't rewrite the table.
set local search_path to rotten, public;

alter table users add column session_generation bigint not null default 0;

comment on column users.session_generation is 'Bumped to end every UI session this user has; a session stores the value it started with.';

-- +goose Down
set local search_path to rotten, public;

alter table users drop column session_generation;
