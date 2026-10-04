-- +goose Up
-- OIDC login matches users on (provider, provider_uid), so a pair must belong
-- to one user. Password users have no provider_uid, hence the partial index.
--
-- Existing duplicates stop the migration instead of being merged: which row
-- keeps the identity decides who gets that person's role and audit history,
-- so an operator has to choose. See docs/database.md. users is small, so a
-- plain (not concurrent) index build is quick.
set local search_path to rotten, public;

-- +goose StatementBegin
do $$
declare
    dupes bigint;
begin
    select count(*) into dupes from (
        select 1 from users
        where provider_uid is not null
        group by provider, provider_uid
        having count(*) > 1
    ) d;
    if dupes > 0 then
        raise exception 'rotten.users has % (provider, provider_uid) pair% shared by more than one user; resolve them before migration 0009 (see docs/database.md)',
            dupes, case when dupes = 1 then '' else 's' end
            using hint = 'Find them with: select provider, provider_uid, array_agg(id) from rotten.users where provider_uid is not null group by 1, 2 having count(*) > 1; then clear provider_uid on, or delete, all but one user in each pair and rerun migrate.';
    end if;
end
$$;
-- +goose StatementEnd

create unique index users_provider_uid_key on users (provider, provider_uid) where provider_uid is not null;

-- +goose Down
set local search_path to rotten, public;

drop index users_provider_uid_key;
