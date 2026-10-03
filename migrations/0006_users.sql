-- +goose Up
-- UI users. Email is plain text with a unique index on lower(email), rather
-- than citext: the UI runs with search_path rotten, where an extension type
-- installed in public wouldn't resolve its case-insensitive operators. The UI
-- normalizes email to lowercase before writing and querying.
set local search_path to rotten, public;

create table users (
    id bigserial primary key,
    email text not null,
    name text,
    provider text,
    provider_uid text,
    password_digest text,
    role text not null default 'viewer' check (role in ('viewer', 'admin')),
    groups text[] not null default '{}',
    active boolean not null default true,
    last_login_at timestamptz
);

create unique index users_email_lower_key on users (lower(email));

comment on column users.id is 'Primary key for UI users.';
comment on column users.email is 'Email address used to identify the user; the UI stores it lowercased, and uniqueness is case-insensitive.';
comment on column users.name is 'Display name from the identity provider or password admin.';
comment on column users.provider is 'Authentication provider name for externally authenticated users.';
comment on column users.provider_uid is 'Provider-specific stable user identifier.';
comment on column users.password_digest is 'bcrypt password digest for password auth; null for OIDC users.';
comment on column users.role is 'Authorization role: viewer or admin.';
comment on column users.groups is 'External identity provider groups observed at last login.';
comment on column users.active is 'Local kill switch; inactive users are logged out on their next request.';
comment on column users.last_login_at is 'Time this user last completed authentication.';

-- +goose Down
set local search_path to rotten, public;

drop table users;
