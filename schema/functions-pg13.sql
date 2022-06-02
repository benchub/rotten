CREATE or replace FUNCTION pg_stat_statements() RETURNS TABLE
(
 userid oid,
 dbid oid ,
 queryid bigint ,
 query text ,
 calls bigint ,
 total_time double precision ,
 min_time double precision ,
 max_time double precision ,
 mean_time double precision ,
 stddev_time double precision,
 rows bigint ,
 shared_blks_hit bigint ,
 shared_blks_read bigint ,
 shared_blks_dirtied bigint ,
 shared_blks_written bigint ,
 local_blks_hit bigint ,
 local_blks_read bigint ,
 local_blks_dirtied bigint,
 local_blks_written bigint ,
 temp_blks_read bigint ,
 temp_blks_written bigint ,
 blk_read_time double precision ,
 blk_write_time double precision )
 AS
$$
BEGIN
  return query select ex.userid,
ex.dbid,
ex.queryid,
ex.query,
ex.calls,
total_plan_time + total_exec_time as total_time,
min_plan_time + min_exec_time as min_time,
max_plan_time + max_exec_time as max_time,
mean_plan_time + mean_exec_time as mean_time,
stddev_plan_time + stddev_exec_time as stddev_time,
ex.rows,
ex.shared_blks_hit,
ex.shared_blks_read,
ex.shared_blks_dirtied,
ex.shared_blks_written,
ex.local_blks_hit,
ex.local_blks_read,
ex.local_blks_dirtied,
ex.local_blks_written,
ex.temp_blks_read,
ex.temp_blks_written,
ex.blk_read_time,
ex.blk_write_time
 from pg_stat_statements ex;
END;
$$ SECURITY DEFINER LANGUAGE plpgsql;


CREATE FUNCTION pg_stat_statements_user_reset() RETURNS void AS
$$
BEGIN
  perform pg_stat_statements_reset();
END;
$$ SECURITY DEFINER LANGUAGE plpgsql;

