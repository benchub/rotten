-- Fingerprint golden corpus. Each case starts with a "-- case: <name>" line;
-- the query is everything up to the next case line (blank lines trimmed).
-- Names share a prefix plus an _a/_b/_c suffix when they're expected to group.
-- fingerprint_test.go runs normalized_fingerprint on each and compares to
-- fingerprints.golden. Regenerate with `make golden` (see the Makefile).

-- case: literal_a
SELECT * FROM users WHERE id = 1
-- case: literal_b
SELECT * FROM users WHERE id = 42
-- case: literal_other_column
SELECT * FROM users WHERE email = 'x@example.com'
-- case: literal_other_table
SELECT * FROM accounts WHERE id = 1

-- case: cursor_declare_a
DECLARE active_record_cursor_abc123 CURSOR FOR SELECT * FROM users
-- case: cursor_declare_b
DECLARE active_record_cursor_zz9y8x CURSOR FOR SELECT * FROM users
-- case: cursor_fetch_a
FETCH 100 FROM active_record_cursor_abc123
-- case: cursor_fetch_b
FETCH 1000 FROM active_record_cursor_zz9y8x
-- case: cursor_close_a
CLOSE active_record_cursor_abc123
-- case: cursor_close_b
CLOSE active_record_cursor_q1w2e3

-- case: temp_table_create_a
CREATE TEMP TABLE users_temp_table_a1b2c3 AS SELECT id FROM users
-- case: temp_table_create_b
CREATE TEMP TABLE users_temp_table_9z8y7x6w AS SELECT id FROM users
-- case: temp_table_select_a
SELECT * FROM users_temp_table_a1b2c3 WHERE id = 5
-- case: temp_table_select_b
SELECT * FROM users_temp_table_qwerty WHERE id = 6
-- case: temp_table_short_suffix
SELECT * FROM users_temp_table_abc WHERE id = 5

-- case: schema_a
SELECT * FROM shard_1.users WHERE id = 1
-- case: schema_b
SELECT * FROM shard_27.users WHERE id = 2
-- case: schema_unqualified
SELECT * FROM users WHERE id = 3
-- case: schema_public
SELECT * FROM public.users WHERE id = 4
-- case: create_schema_a
CREATE SCHEMA shard_1
-- case: create_schema_b
CREATE SCHEMA shard_27
-- case: qualified_column_ref_a
SELECT users.id FROM users
-- case: qualified_column_ref_b
SELECT public.users.id FROM users
-- case: qualified_column_ref_c
SELECT catalog_1.public.users.id FROM users
-- case: qualified_column_ref_d
SELECT catalog_1.shard_27.users.id FROM users
-- case: qualified_function_a
SELECT f(id) FROM users
-- case: qualified_function_b
SELECT shard_1.f(id) FROM users
-- case: qualified_function_c
SELECT shard_27.f(id) FROM users
-- case: sql_syntax_extract
SELECT EXTRACT(YEAR FROM TIMESTAMP '2024-01-02')
-- case: sql_syntax_at_time_zone
SELECT TIMESTAMP '2024-01-02 03:04:05' AT TIME ZONE 'UTC'
-- case: sql_syntax_trim
SELECT TRIM(BOTH 'x' FROM 'xxhelloxx')
-- case: sql_syntax_substring
SELECT SUBSTRING('abcdef' FROM 2 FOR 3)
-- case: drop_table_name_a
DROP TABLE users
-- case: drop_table_name_b
DROP TABLE public.users
-- case: drop_table_name_c
DROP TABLE shard_27.users
-- case: drop_trigger_name_a
DROP TRIGGER trg ON users
-- case: drop_trigger_name_b
DROP TRIGGER trg ON public.users
-- case: drop_trigger_other_table
DROP TRIGGER trg ON public.orders
-- case: drop_policy_name_a
DROP POLICY pol ON users
-- case: drop_policy_name_b
DROP POLICY pol ON public.users
-- case: drop_rule_name_a
DROP RULE rul ON users
-- case: drop_rule_name_b
DROP RULE rul ON public.users
-- case: comment_column_name_a
COMMENT ON COLUMN users.id IS 'x'
-- case: comment_column_name_b
COMMENT ON COLUMN public.users.id IS 'x'
-- case: comment_column_other_table
COMMENT ON COLUMN orders.id IS 'x'
-- case: comment_constraint_name_a
COMMENT ON CONSTRAINT users_pkey ON users IS 'x'
-- case: comment_constraint_name_b
COMMENT ON CONSTRAINT users_pkey ON public.users IS 'x'
-- case: drop_opclass_name_a
DROP OPERATOR CLASS c USING btree
-- case: drop_opclass_name_b
DROP OPERATOR CLASS s.c USING btree
-- case: drop_opfamily_name_a
DROP OPERATOR FAMILY f USING btree
-- case: drop_opfamily_name_b
DROP OPERATOR FAMILY s.f USING btree
-- case: drop_function_name_a
DROP FUNCTION f(int)
-- case: drop_function_name_b
DROP FUNCTION public.f(int)
-- case: drop_function_name_c
DROP FUNCTION shard_27.f(int)
-- case: qualified_type_name_a
SELECT id::mytype FROM users
-- case: qualified_type_name_b
SELECT id::public.mytype FROM users
-- case: qualified_type_name_c
SELECT id::shard_27.mytype FROM users
-- case: pct_type_arg_a
CREATE FUNCTION f(x users.id%TYPE) RETURNS int LANGUAGE sql AS 'SELECT 1'
-- case: pct_type_arg_b
CREATE FUNCTION f(x public.users.id%TYPE) RETURNS int LANGUAGE sql AS 'SELECT 1'
-- case: pct_type_arg_other_table
CREATE FUNCTION f(x orders.id%TYPE) RETURNS int LANGUAGE sql AS 'SELECT 1'
-- case: pct_type_returns_a
CREATE FUNCTION f() RETURNS users.id%TYPE LANGUAGE sql AS 'SELECT 1'
-- case: pct_type_returns_b
CREATE FUNCTION f() RETURNS public.users.id%TYPE LANGUAGE sql AS 'SELECT 1'
-- case: pct_type_returns_other_table
CREATE FUNCTION f() RETURNS orders.id%TYPE LANGUAGE sql AS 'SELECT 1'
-- case: alter_set_schema_a
ALTER TABLE users SET SCHEMA tenant_1
-- case: alter_set_schema_b
ALTER TABLE users SET SCHEMA tenant_27

-- case: repack_index_a
CREATE INDEX CONCURRENTLY index_12345 ON repack.table_12345 (id)
-- case: repack_index_b
CREATE INDEX CONCURRENTLY index_67890 ON repack.table_67890 (id)
-- case: repack_table_a
INSERT INTO repack.table_12345 SELECT * FROM users
-- case: repack_table_b
INSERT INTO repack.table_999 SELECT * FROM users
-- case: repack_named_index
CREATE INDEX CONCURRENTLY index_users_on_email ON users (email)

-- case: set_role_a
SET ROLE alice
-- case: set_role_b
SET ROLE bob
-- case: reset_role
RESET ROLE

-- case: in_list_a
SELECT * FROM users WHERE id IN (1)
-- case: in_list_b
SELECT * FROM users WHERE id IN (1, 2, 3)
-- case: in_list_c
SELECT * FROM users WHERE id IN (4, 5, 6, 7, 8, 9, 10)
-- case: in_list_d
SELECT * FROM users WHERE id IN ($1 /*, ... */)
-- case: in_list_e
SELECT * FROM users WHERE id IN ($1)
-- case: in_list_f
SELECT * FROM users WHERE id IN ($1,$2,$3)
-- = ANY(ARRAY[...]) shares a Postgres 18 queryid with IN (...), so it groups too.
-- case: in_list_any_a
SELECT * FROM users WHERE id = ANY(ARRAY[1])
-- case: in_list_any_b
SELECT * FROM users WHERE id = ANY(ARRAY[1, 2, 3])
-- case: in_list_any_c
SELECT * FROM users WHERE id = any(array[4, 5, 6, 7, 8, 9, 10])
-- case: in_list_any_d
SELECT * FROM users WHERE id = ANY(ARRAY[$1 /*, ... */])
-- case: in_list_any_e
SELECT * FROM users WHERE id = ANY(ARRAY[$1, $2, $3])
-- case: in_list_any_param
SELECT * FROM users WHERE id = ANY($1)
-- A cast array is the IN list with the cast pushed onto each element, which
-- is how Postgres resolves it, so it groups too, whatever the cast type.
-- case: in_list_any_cast_a
SELECT * FROM users WHERE id = ANY(ARRAY[1, 2, 3]::int[])
-- case: any_array_cast
SELECT * FROM users WHERE id = ANY(ARRAY[1, 2, 3]::bigint[])
-- case: in_list_any_cast_b
SELECT * FROM users WHERE id = ANY(ARRAY[$1 /*, ... */]::bigint[])
-- A domain array is coerced as a whole, not per element, so this merge is
-- an accepted over-merge.
-- case: in_list_any_cast_domain
SELECT * FROM users WHERE id = ANY(ARRAY[1, 2, 3]::posint[])

-- NOT IN (...) and <> ALL(ARRAY[...]) group with each other (and with <> 1).
-- case: not_in_list_a
SELECT * FROM users WHERE id NOT IN (1, 2, 3)
-- case: not_in_list_b
SELECT * FROM users WHERE id <> ALL(ARRAY[1, 2, 3])
-- case: not_in_list_c
SELECT * FROM users WHERE id != ALL(ARRAY[$1 /*, ... */])
-- case: not_in_list_cast
SELECT * FROM users WHERE id <> ALL(ARRAY[1, 2, 3]::int[])

-- Other ANY/ALL forms stay distinct from IN and from each other.
-- case: any_lt_array
SELECT * FROM users WHERE id < ANY(ARRAY[1, 2, 3])
-- case: all_eq_array
SELECT * FROM users WHERE id = ALL(ARRAY[1, 2, 3])
-- case: any_ne_array
SELECT * FROM users WHERE id <> ANY(ARRAY[1, 2, 3])
-- case: any_nested_array
SELECT * FROM users WHERE id = ANY(ARRAY[[1, 2], [3, 4]])
-- case: any_nested_array_cast
SELECT * FROM users WHERE id = ANY(ARRAY[[1, 2], [3, 4]]::int[])

-- IN (subquery) and = ANY(subquery) are the same SubLink to Postgres.
-- case: in_subquery_a
SELECT * FROM users WHERE id IN (SELECT user_id FROM accounts)
-- case: in_subquery_b
SELECT * FROM users WHERE id = ANY(SELECT user_id FROM accounts)
-- case: any_lt_subquery
SELECT * FROM users WHERE id < ANY(SELECT user_id FROM accounts)

-- case: values_a
INSERT INTO users (id, name) VALUES (1, 'a')
-- case: values_b
INSERT INTO users (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')

-- case: marginalia_a
SELECT * FROM users WHERE id = 1 /*application:web,controller:users,action:show*/
-- case: marginalia_b
SELECT * FROM users WHERE id = 9 /*application:jobs,job:SyncUsers*/
-- case: marginalia_leading
/*application:web*/ SELECT * FROM users WHERE id = 1

-- case: multi_statement
SELECT 1; SELECT 2

-- case: parse_error_garbage
SELEC * FROM users
-- case: parse_error_truncated
SELECT * FROM users WHERE id =
-- case: parse_error_pgss_ellipsis
SELECT * FROM users WHERE id IN ($1, $2, ...)
-- case: pgss_normalized_param
SELECT * FROM users WHERE id = $1

-- Postgres 18 syntax. The PG17 parser accepts OLD/NEW column references
-- (without understanding their new semantics), but rejects the other cases.
-- case: pg18_returning_old_new
UPDATE users SET name = 'updated' WHERE id = 1 RETURNING OLD.name, NEW.name
-- case: pg18_returning_with_aliases
UPDATE users SET name = 'updated' WHERE id = 1 RETURNING WITH (OLD AS o, NEW AS n) o.name, n.name
-- case: pg18_generated_virtual
CREATE TABLE virtual_explicit (id int, doubled int GENERATED ALWAYS AS (id * 2) VIRTUAL)
-- case: pg18_generated_default_virtual
CREATE TABLE virtual_default (id int, doubled int GENERATED ALWAYS AS (id * 2))
-- case: pg18_without_overlaps_primary
CREATE TABLE temporal_primary (id int, valid_at daterange, PRIMARY KEY (id, valid_at WITHOUT OVERLAPS))
-- case: pg18_without_overlaps_unique
CREATE TABLE temporal_unique (id int, valid_at daterange, UNIQUE (id, valid_at WITHOUT OVERLAPS))
