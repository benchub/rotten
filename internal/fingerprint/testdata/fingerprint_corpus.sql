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

-- NOT IN (...) and <> ALL(ARRAY[...]) group with each other (and with <> 1).
-- case: not_in_list_a
SELECT * FROM users WHERE id NOT IN (1, 2, 3)
-- case: not_in_list_b
SELECT * FROM users WHERE id <> ALL(ARRAY[1, 2, 3])
-- case: not_in_list_c
SELECT * FROM users WHERE id != ALL(ARRAY[$1 /*, ... */])

-- Other ANY/ALL forms stay distinct from IN and from each other.
-- case: any_lt_array
SELECT * FROM users WHERE id < ANY(ARRAY[1, 2, 3])
-- case: all_eq_array
SELECT * FROM users WHERE id = ALL(ARRAY[1, 2, 3])
-- case: any_ne_array
SELECT * FROM users WHERE id <> ANY(ARRAY[1, 2, 3])
-- case: any_array_cast
SELECT * FROM users WHERE id = ANY(ARRAY[1, 2, 3]::bigint[])
-- case: any_nested_array
SELECT * FROM users WHERE id = ANY(ARRAY[[1, 2], [3, 4]])

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
