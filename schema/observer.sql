-- Sets up the role the rotten worker uses to read pg_stat_statements on an
-- observed database (Postgres 14 through 18). Run it as a superuser with psql,
-- in the database where pg_stat_statements is installed:
--
--   psql -v observer_role=rotten_observer -v observer_schema=rotten -f schema/observer.sql
--
-- observer_role defaults to rotten_observer, and observer_schema (where the 17+
-- min/max reset wrapper lives) defaults to rotten. The script is safe to rerun.
-- It doesn't set a password; do that yourself (or use another auth method).

\set ON_ERROR_STOP on

\if :{?observer_role}
\else
  \set observer_role rotten_observer
\endif
\if :{?observer_schema}
\else
  \set observer_schema rotten
\endif

-- On 17+, the worker resets min and max after each harvest. The raw
-- pg_stat_statements_reset(oid,oid,bigint,boolean) can't be granted: every
-- argument has a default and minmax_only defaults to false, so EXECUTE on it
-- also allows a full reset. Instead we create one SECURITY DEFINER wrapper,
-- <observer_schema>.pg_stat_statements_minmax_reset(), that only runs
-- pg_stat_statements_reset(0, 0, 0, true). On 14 through 16 there's no
-- min/max reset, so we skip it. On 17+ the extension must be at 1.11 or later;
-- if it isn't (say, after a pg_upgrade), the script stops and says to run
-- ALTER EXTENSION pg_stat_statements UPDATE.
select current_setting('server_version_num')::int >= 170000 as pg17plus \gset

\if :pg17plus
  -- Checks first, so a failure changes nothing. Each check raises an error
  -- through a generated DO block, since psql variables aren't expanded inside
  -- dollar quotes.
  select format('do $do$begin raise exception %L; end$do$',
                'pg_stat_statements is not installed in this database')
  where not exists (select from pg_extension where extname = 'pg_stat_statements') \gexec

  -- The four-argument reset arrived in extension version 1.11. After a
  -- pg_upgrade to 17+, the extension stays at its old version until someone
  -- updates it.
  select format('do $do$begin raise exception using errcode = %L, message = %L; end$do$',
                'object_not_in_prerequisite_state',
                format('pg_stat_statements is at version %s, but Postgres 17 and later need 1.11 or newer; run ALTER EXTENSION pg_stat_statements UPDATE',
                       extversion))
  from pg_extension
  where extname = 'pg_stat_statements'
    and string_to_array(extversion, '.')::int[] < '{1,11}' \gexec

  -- The wrapper runs as us, so nobody else may own its schema or create
  -- objects in it.
  select format('do $do$begin raise exception %L; end$do$',
                format('schema %I exists and is owned by %s, not %s',
                       n.nspname, n.nspowner::regrole, current_user))
  from pg_namespace n
  where n.nspname = :'observer_schema' and n.nspowner <> current_user::regrole \gexec

  select format('do $do$begin raise exception %L; end$do$',
                format('role %I has CREATE on schema %I', r.rolname, n.nspname))
  from pg_namespace n, pg_roles r
  where n.nspname = :'observer_schema'
    and r.oid <> n.nspowner and not r.rolsuper
    and has_schema_privilege(r.oid, n.oid, 'CREATE')
  limit 1 \gexec

  -- CREATE OR REPLACE keeps an existing function's owner, so refuse a wrapper
  -- someone else already owns.
  select format('do $do$begin raise exception %L; end$do$',
                format('function %I.pg_stat_statements_minmax_reset() exists and is owned by %s, not %s',
                       n.nspname, p.proowner::regrole, current_user))
  from pg_proc p join pg_namespace n on n.oid = p.pronamespace
  where n.nspname = :'observer_schema'
    and p.proname = 'pg_stat_statements_minmax_reset'
    and p.proowner <> current_user::regrole
  limit 1 \gexec

  select n.nspname as pgss_schema
  from pg_extension e join pg_namespace n on n.oid = e.extnamespace
  where e.extname = 'pg_stat_statements' \gset
\endif

select format('create role %I login', :'observer_role')
where not exists (select from pg_roles where rolname = :'observer_role') \gexec

-- Lets the observer see every role's rows and query text.
select format('grant pg_read_all_stats to %I', :'observer_role') \gexec

\if :pg17plus
  select format('create schema if not exists %I', :'observer_schema') \gexec
  select format('grant usage on schema %I to %I', :'observer_schema', :'observer_role') \gexec

  -- BEGIN ATOMIC binds the call when the function is created, not when it
  -- runs, and the explicit casts make the real (oid,oid,bigint,boolean)
  -- function the exact match. Together they stop anyone with CREATE in the
  -- pg_stat_statements schema from slipping in a better-matching overload
  -- (say, (int,int,int,bool)) that would then run as us.
  select format($f$
    create or replace function %1$I.pg_stat_statements_minmax_reset()
    returns timestamptz
    language sql
    security definer
    set search_path = pg_catalog, %2$I
    begin atomic
      select %2$I.pg_stat_statements_reset(0::oid, 0::oid, 0::bigint, true);
    end
  $f$, :'observer_schema', :'pgss_schema') \gexec

  select format('revoke all on function %I.pg_stat_statements_minmax_reset() from public', :'observer_schema') \gexec
  select format('grant execute on function %I.pg_stat_statements_minmax_reset() to %I',
                :'observer_schema', :'observer_role') \gexec
\endif
