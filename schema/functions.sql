CREATE FUNCTION pg_stat_statements() RETURNS SETOF pg_stat_statements AS
$$
BEGIN
  return query select * from pg_stat_statements;
END;
$$ SECURITY DEFINER LANGUAGE plpgsql;


CREATE FUNCTION pg_stat_statements_user_reset() RETURNS void AS
$$
BEGIN
  perform pg_stat_statements_reset();
END;
$$ SECURITY DEFINER LANGUAGE plpgsql;

