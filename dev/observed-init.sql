CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS pg_stat_statement_context;
CREATE ROLE rotten_observer LOGIN PASSWORD 'rotten_observer';
GRANT pg_read_all_stats TO rotten_observer;

CREATE SCHEMA IF NOT EXISTS rotten;
GRANT USAGE ON SCHEMA rotten TO rotten_observer;

CREATE OR REPLACE FUNCTION rotten.pg_stat_statements_minmax_reset()
RETURNS timestamptz
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, public
BEGIN ATOMIC
  SELECT public.pg_stat_statements_reset(0::oid, 0::oid, 0::bigint, true);
END;

REVOKE ALL ON FUNCTION rotten.pg_stat_statements_minmax_reset() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION rotten.pg_stat_statements_minmax_reset() TO rotten_observer;
