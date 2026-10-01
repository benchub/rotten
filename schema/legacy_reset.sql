-- TEMPORARY: removed in task 20261001-103222-25, when diffing replaces resets.
--
-- Until then the worker full-resets pg_stat_statements after each window
-- through dba.pg_stat_statements_user_reset(). Reads don't use the dba schema;
-- the worker reads pg_stat_statements directly as the observer role. Run this
-- as a superuser after schema/observer.sql, in the same database:
--
--   psql -v observer_role=rotten_observer -f schema/legacy_reset.sql
--
-- The function runs as us, so it follows the same hardening as the min/max
-- wrapper in observer.sql. See the comments there.

\set ON_ERROR_STOP on

\if :{?observer_role}
\else
  \set observer_role rotten_observer
\endif

select current_setting('server_version_num')::int >= 170000 as pg17plus \gset

-- Checks first, so a failure changes nothing.
select format('do $do$begin raise exception %L; end$do$',
              'pg_stat_statements is not installed in this database')
where not exists (select from pg_extension where extname = 'pg_stat_statements') \gexec

select format('do $do$begin raise exception %L; end$do$',
              format('schema dba exists and is owned by %s, not %s', n.nspowner::regrole, current_user))
from pg_namespace n
where n.nspname = 'dba' and n.nspowner <> current_user::regrole \gexec

select format('do $do$begin raise exception %L; end$do$',
              format('role %I has CREATE on schema dba', r.rolname))
from pg_namespace n, pg_roles r
where n.nspname = 'dba'
  and r.oid <> n.nspowner and not r.rolsuper
  and has_schema_privilege(r.oid, n.oid, 'CREATE')
limit 1 \gexec

select format('do $do$begin raise exception %L; end$do$',
              format('function dba.pg_stat_statements_user_reset() exists and is owned by %s, not %s',
                     p.proowner::regrole, current_user))
from pg_proc p join pg_namespace n on n.oid = p.pronamespace
where n.nspname = 'dba'
  and p.proname = 'pg_stat_statements_user_reset'
  and p.proowner <> current_user::regrole
limit 1 \gexec

select n.nspname as pgss_schema
from pg_extension e join pg_namespace n on n.oid = e.extnamespace
where e.extname = 'pg_stat_statements' \gset

create schema if not exists dba;

-- An older install made this a plpgsql function returning void. Drop it so
-- the new one can replace it (we own it, per the check above).
drop function if exists dba.pg_stat_statements_user_reset();

-- BEGIN ATOMIC binds the call at create time, and the explicit casts make the
-- real function the exact match, so no overload planted later can run as us.
\if :pg17plus
  select format($f$
    create function dba.pg_stat_statements_user_reset()
    returns void
    language sql
    security definer
    set search_path = pg_catalog, %1$I
    begin atomic
      select %1$I.pg_stat_statements_reset(0::oid, 0::oid, 0::bigint, false);
    end
  $f$, :'pgss_schema') \gexec
\else
  select format($f$
    create function dba.pg_stat_statements_user_reset()
    returns void
    language sql
    security definer
    set search_path = pg_catalog, %1$I
    begin atomic
      select %1$I.pg_stat_statements_reset(0::oid, 0::oid, 0::bigint);
    end
  $f$, :'pgss_schema') \gexec
\endif

revoke all on function dba.pg_stat_statements_user_reset() from public;
select format('grant usage on schema dba to %I', :'observer_role') \gexec
select format('grant execute on function dba.pg_stat_statements_user_reset() to %I', :'observer_role') \gexec
