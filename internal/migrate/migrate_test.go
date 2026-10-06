package migrate_test

import (
	"context"
	"errors"
	"maps"
	"path"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/benchub/rotten/internal/migrate"
	"github.com/benchub/rotten/internal/testdb"
	"github.com/benchub/rotten/migrations"
)

// schemaSnapshot lists every relation, column, and type in the rotten
// schema, so a second migrate can be shown to change nothing.
const schemaSnapshot = `
select coalesce(string_agg(x, E'\n' order by x), '') from (
	select c.relname || ':' || c.relkind::text || ':' || pg_get_userbyid(c.relowner) as x
	from pg_class c join pg_namespace n on n.oid = c.relnamespace
	where n.nspname = 'rotten'
	union all
	select table_name || '.' || column_name || ':' || data_type
	from information_schema.columns where table_schema = 'rotten'
	union all
	select 'version:' || max(version_id)::text || ':' || count(*)::text from public.goose_db_version
) s`

func TestMigrateUpFromEmptyIsIdempotent(t *testing.T) {
	db := testdb.StartRottenEmpty(t)
	ctx := context.Background()
	dsn := db.DSNAs(t, testdb.OwnerRole)

	applied, err := migrate.Up(ctx, dsn)
	if err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	if len(applied) == 0 || applied[0] != 1 {
		t.Fatalf("first migrate applied %v, want it to start at version 1", applied)
	}

	conn := db.Connect(t)
	var before string
	if err := conn.QueryRow(ctx, schemaSnapshot).Scan(&before); err != nil {
		t.Fatal(err)
	}
	var owner string
	if err := conn.QueryRow(ctx, "select pg_get_userbyid(relowner) from pg_class where oid = 'rotten.events'::regclass").Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if owner != testdb.OwnerRole {
		t.Errorf("rotten.events owner = %q, want %q", owner, testdb.OwnerRole)
	}

	again, err := migrate.Up(ctx, dsn)
	if err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	if len(again) != 0 {
		t.Errorf("second migrate applied %v, want nothing", again)
	}
	var after string
	if err := conn.QueryRow(ctx, schemaSnapshot).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Errorf("second migrate changed the schema:\nbefore:\n%s\nafter:\n%s", before, after)
	}
}

func TestFingerprintStatsLastHoldsBigint(t *testing.T) {
	db := testdb.StartRotten(t)
	conn := db.Connect(t)
	ctx := context.Background()
	const big = int64(1) << 31
	_, err := conn.Exec(ctx, `
		with f as (insert into rotten.fingerprints (fingerprint, normalized) values ('q', 'q') returning id)
		insert into rotten.fingerprint_stats (fingerprint_id, logical_source_id, type, last)
		select id, 0, 'calls', $1 from f`, big)
	if err != nil {
		t.Fatalf("write last = 2^31: %v", err)
	}
	var got int64
	if err := conn.QueryRow(ctx, "select last from rotten.fingerprint_stats").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != big {
		t.Errorf("last = %d, want %d", got, big)
	}
}

func TestFingerprintColumnCommentsDescribeStoredValues(t *testing.T) {
	db := testdb.StartRotten(t)
	conn := db.Connect(t)
	ctx := context.Background()

	want := map[string]string{
		"fingerprint": "Hex string from fingerprint.Normalized, or unparsed- plus a hash of the query text when the parser rejected it (see unparsed); matches proto.rotten.v1.FingerprintAggregate.fingerprint.",
		"normalized":  "pg_query.Normalize output of one representative query text for this fingerprint, or the pg_stat_statements text with leading and trailing comments stripped when unparsed; stored only on first insert.",
		"unparsed":    "True when the worker's parser rejected the statement, so fingerprint is a hash of its text rather than a pg_query fingerprint. Set on first insert, or later for an unparsed- fingerprint an older server stored; never cleared. False for workers older than the flag.",
	}
	for column, comment := range want {
		var got string
		if err := conn.QueryRow(ctx, `
			select col_description('rotten.fingerprints'::regclass, a.attnum)
			from pg_attribute a
			where a.attrelid = 'rotten.fingerprints'::regclass
			  and a.attname = $1
			  and not a.attisdropped`, column).Scan(&got); err != nil {
			t.Fatalf("%s comment: %v", column, err)
		}
		if got != comment {
			t.Errorf("%s comment = %q, want %q", column, got, comment)
		}
	}
}

func TestUsersTableSchema(t *testing.T) {
	db := testdb.StartRotten(t)
	conn := db.Connect(t)
	ctx := context.Background()

	if _, err := conn.Exec(ctx, `insert into rotten.users (email, name, role, active)
		values ('viewer@example.com', 'Viewer', 'viewer', true)`); err != nil {
		t.Fatalf("insert minimal user: %v", err)
	}
	if _, err := conn.Exec(ctx, `insert into rotten.users (email, role)
		values ('VIEWER@example.com', 'viewer')`); err == nil {
		t.Fatal("case-insensitive duplicate email insert succeeded")
	}
	// The UI connects with search_path rotten only, so uniqueness must not
	// depend on anything installed in public.
	if _, err := conn.Exec(ctx, `set search_path to rotten`); err != nil {
		t.Fatalf("set search_path: %v", err)
	}
	if _, err := conn.Exec(ctx, `insert into users (email, role)
		values ('Viewer@Example.com', 'viewer')`); err == nil {
		t.Fatal("case-insensitive duplicate email insert succeeded with search_path rotten")
	}
	if _, err := conn.Exec(ctx, `reset search_path`); err != nil {
		t.Fatalf("reset search_path: %v", err)
	}
	if _, err := conn.Exec(ctx, `insert into rotten.users (email, role)
		values ('admin@example.com', 'admin')`); err != nil {
		t.Fatalf("insert admin user: %v", err)
	}
	if _, err := conn.Exec(ctx, `insert into rotten.users (email, role)
		values ('bad@example.com', 'owner')`); err == nil {
		t.Fatal("insert with invalid role succeeded")
	}

	wantTypes := map[string]string{
		"id":                 "bigint",
		"email":              "text",
		"name":               "text",
		"provider":           "text",
		"provider_uid":       "text",
		"password_digest":    "text",
		"role":               "text",
		"groups":             "text[]",
		"active":             "boolean",
		"last_login_at":      "timestamp with time zone",
		"session_generation": "bigint",
	}
	for column, want := range wantTypes {
		var got string
		if err := conn.QueryRow(ctx, `
			select format_type(a.atttypid, a.atttypmod)
			from pg_attribute a
			where a.attrelid = 'rotten.users'::regclass
			  and a.attname = $1
			  and not a.attisdropped`, column).Scan(&got); err != nil {
			t.Fatalf("%s type: %v", column, err)
		}
		if got != want {
			t.Errorf("%s type = %s, want %s", column, got, want)
		}
	}
}

func TestUsersProviderUIDIsUnique(t *testing.T) {
	db := testdb.StartRotten(t)
	conn := db.Connect(t)
	ctx := context.Background()

	insert := `insert into rotten.users (email, provider, provider_uid) values ($1, $2, $3)`
	if _, err := conn.Exec(ctx, insert, "first@example.com", "oidc:a", "sub-1"); err != nil {
		t.Fatalf("insert first identity: %v", err)
	}
	_, err := conn.Exec(ctx, insert, "second@example.com", "oidc:a", "sub-1")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" || pgErr.ConstraintName != "users_provider_uid_key" {
		t.Fatalf("duplicate (provider, provider_uid): err %v, want a unique violation on users_provider_uid_key", err)
	}

	for what, args := range map[string][]any{
		"same sub, other provider":     {"other@example.com", "oidc:b", "sub-1"},
		"no sub (password user)":       {"pw1@example.com", "password", nil},
		"another user with no sub":     {"pw2@example.com", "password", nil},
		"same provider, different sub": {"third@example.com", "oidc:a", "sub-2"},
	} {
		if _, err := conn.Exec(ctx, insert, args...); err != nil {
			t.Errorf("%s: %v", what, err)
		}
	}
}

// Migration 0009 refuses to run over duplicate identities rather than pick
// which row keeps one: they're login data, so a person has to decide.
func TestUsersProviderUIDMigrationRefusesExistingDuplicates(t *testing.T) {
	db := testdb.StartRottenEmpty(t)
	ctx := context.Background()
	dsn := db.DSNAs(t, testdb.OwnerRole)

	before := migrationsBefore(t, usersProviderUIDMigration)
	if _, err := migrate.UpWith(ctx, dsn, before, migrations.Permissions); err != nil {
		t.Fatalf("migrate to 0008: %v", err)
	}
	conn := db.Connect(t)
	if _, err := conn.Exec(ctx, `insert into rotten.users (email, provider, provider_uid) values
		('one@example.com', 'oidc:a', 'sub-dup'), ('two@example.com', 'oidc:a', 'sub-dup')`); err != nil {
		t.Fatalf("insert duplicates: %v", err)
	}

	_, err := migrate.Up(ctx, dsn)
	if err == nil || !strings.Contains(err.Error(), "rotten.users has 1 (provider, provider_uid) pair shared by more than one user") {
		t.Fatalf("migrate over duplicates: err %v, want a clear duplicate-identity error", err)
	}
	var version int64
	if err := conn.QueryRow(ctx, "select max(version_id) from public.goose_db_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 8 {
		t.Errorf("version after failed migrate = %d, want 8", version)
	}

	if _, err := conn.Exec(ctx, `update rotten.users set provider_uid = null where email = 'two@example.com'`); err != nil {
		t.Fatal(err)
	}
	// Later migrations apply in the same run, so only 9 itself is checked.
	if applied, err := migrate.Up(ctx, dsn); err != nil || !slices.Contains(applied, 9) {
		t.Fatalf("migrate after fixing duplicates: applied %v, err %v; want it to include 9", applied, err)
	}
}

const usersProviderUIDMigration = "0009_users_provider_uid_unique.sql"

// migrationsBefore copies the migrations without name and every migration
// after it, so a test can stop the schema just before name, however many
// migrations are added later.
func migrationsBefore(t *testing.T, name string) fstest.MapFS {
	t.Helper()
	m := copyMigrations(t)
	if _, ok := m[name]; !ok {
		t.Fatalf("migration %s not found", name)
	}
	for p := range m {
		if path.Ext(p) == ".sql" && p >= name {
			delete(m, p)
		}
	}
	return m
}

const usersSessionGenerationMigration = "0010_users_session_generation.sql"

// users.session_generation is what the UI bumps to end every session a user
// has. Users from before the migration start at 0, like new ones.
func TestUsersSessionGeneration(t *testing.T) {
	db := testdb.StartRottenEmpty(t)
	ctx := context.Background()
	dsn := db.DSNAs(t, testdb.OwnerRole)

	before := migrationsBefore(t, usersSessionGenerationMigration)
	if _, err := migrate.UpWith(ctx, dsn, before, migrations.Permissions); err != nil {
		t.Fatalf("migrate to 0009: %v", err)
	}
	conn := db.Connect(t)
	if _, err := conn.Exec(ctx, `insert into rotten.users (email) values ('existing@example.com')`); err != nil {
		t.Fatalf("insert existing user: %v", err)
	}
	if applied, err := migrate.Up(ctx, dsn); err != nil || !slices.Contains(applied, 10) {
		t.Fatalf("migrate to 0010: applied %v, err %v; want it to include 10", applied, err)
	}

	var dataType, comment string
	var notNull bool
	var def *string
	if err := conn.QueryRow(ctx, `
		select format_type(a.atttypid, a.atttypmod), a.attnotnull, pg_get_expr(d.adbin, d.adrelid),
			coalesce(col_description(a.attrelid, a.attnum), '')
		from pg_attribute a
		left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
		where a.attrelid = 'rotten.users'::regclass
		  and a.attname = 'session_generation'
		  and not a.attisdropped`).Scan(&dataType, &notNull, &def, &comment); err != nil {
		t.Fatalf("session_generation column: %v", err)
	}
	if dataType != "bigint" || !notNull || def == nil || *def != "0" {
		t.Errorf("session_generation is %s, not null %v, default %v; want bigint not null default 0", dataType, notNull, def)
	}
	if comment == "" {
		t.Error("users.session_generation has no comment")
	}

	if _, err := conn.Exec(ctx, `insert into rotten.users (email) values ('new@example.com')`); err != nil {
		t.Fatalf("insert new user: %v", err)
	}
	rows, err := conn.Query(ctx, `select email, session_generation from rotten.users order by email`)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int64{}
	for rows.Next() {
		var email string
		var generation int64
		if err := rows.Scan(&email, &generation); err != nil {
			t.Fatal(err)
		}
		got[email] = generation
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{"existing@example.com": 0, "new@example.com": 0}
	if !maps.Equal(got, want) {
		t.Errorf("session generations = %v, want %v", got, want)
	}
	if _, err := conn.Exec(ctx, `update rotten.users set session_generation = null`); err == nil {
		t.Error("setting session_generation to null succeeded")
	}
}

func TestUsersColumnCommentsDescribeValues(t *testing.T) {
	db := testdb.StartRotten(t)
	conn := db.Connect(t)
	ctx := context.Background()

	want := map[string]string{
		"id":                 "Primary key for UI users.",
		"email":              "Email address used to identify the user; the UI stores it lowercased, and uniqueness is case-insensitive.",
		"name":               "Display name from the identity provider or password admin.",
		"provider":           "Authentication provider name for externally authenticated users.",
		"provider_uid":       "Provider-specific stable user identifier.",
		"password_digest":    "bcrypt password digest for password auth; null for OIDC users.",
		"role":               "Authorization role: viewer or admin.",
		"groups":             "External identity provider groups observed at last login.",
		"active":             "Local kill switch; inactive users are logged out on their next request.",
		"last_login_at":      "Time this user last completed authentication.",
		"session_generation": "Bumped to end every UI session this user has; a session stores the value it started with.",
	}
	for column, comment := range want {
		var got string
		if err := conn.QueryRow(ctx, `
			select col_description('rotten.users'::regclass, a.attnum)
			from pg_attribute a
			where a.attrelid = 'rotten.users'::regclass
			  and a.attname = $1
			  and not a.attisdropped`, column).Scan(&got); err != nil {
			t.Fatalf("%s comment: %v", column, err)
		}
		if got != comment {
			t.Errorf("%s comment = %q, want %q", column, got, comment)
		}
	}
}

func TestUIAuditLogSchema(t *testing.T) {
	db := testdb.StartRotten(t)
	conn := db.Connect(t)
	ctx := context.Background()

	var id int64
	var stamped bool
	var details string
	if err := conn.QueryRow(ctx, `insert into rotten.ui_audit_log (actor_email, action)
		values ('admin@example.com', 'api_key.create')
		returning id, at between now() - interval '1 minute' and now(), details::text`).Scan(&id, &stamped, &details); err != nil {
		t.Fatalf("insert minimal audit row: %v", err)
	}
	if id <= 0 || !stamped || details != "{}" {
		t.Errorf("defaults: id %d, at stamped %v, details %s; want a positive id, now(), {}", id, stamped, details)
	}
	for what, q := range map[string]string{
		"no actor email": `insert into rotten.ui_audit_log (action) values ('api_key.create')`,
		"empty action":   `insert into rotten.ui_audit_log (actor_email, action) values ('a@example.com', '')`,
		"odd action":     `insert into rotten.ui_audit_log (actor_email, action) values ('a@example.com', 'Drop Table')`,
		"no action":      `insert into rotten.ui_audit_log (actor_email) values ('a@example.com')`,
		"details array":  `insert into rotten.ui_audit_log (actor_email, action, details) values ('a@example.com', 'api_key.create', '[]')`,
	} {
		if _, err := conn.Exec(ctx, q); err == nil {
			t.Errorf("%s: insert succeeded, want a constraint violation", what)
		}
	}
	// The log has no foreign key to users, so deleting a user keeps the
	// history, with the actor's email.
	if _, err := conn.Exec(ctx, `insert into rotten.ui_audit_log (actor_user_id, actor_email, action, target_type, target_id)
		values (424242, 'gone@example.com', 'api_key.revoke', 'api_key', 1)`); err != nil {
		t.Errorf("audit row for a user and key that don't exist: %v", err)
	}

	for _, column := range []string{"id", "at", "actor_user_id", "actor_email", "action", "target_type", "target_id", "details"} {
		var got *string
		if err := conn.QueryRow(ctx, `
			select col_description('rotten.ui_audit_log'::regclass, a.attnum)
			from pg_attribute a
			where a.attrelid = 'rotten.ui_audit_log'::regclass
			  and a.attname = $1
			  and not a.attisdropped`, column).Scan(&got); err != nil {
			t.Fatalf("%s comment: %v", column, err)
		}
		if got == nil || *got == "" {
			t.Errorf("ui_audit_log.%s has no comment", column)
		}
	}
}

func TestEventContextCountIsBigintEverywhere(t *testing.T) {
	db := testdb.StartRotten(t)
	conn := db.Connect(t)
	ctx := context.Background()

	relations := []string{"rotten.event_context", "rotten.event_context_partition_template"}
	var partition string
	if err := conn.QueryRow(ctx, `
		select (c.oid::regclass)::text
		from pg_inherits i
		join pg_class c on c.oid = i.inhrelid
		where i.inhparent = 'rotten.event_context'::regclass
		order by c.relname
		limit 1`).Scan(&partition); err != nil {
		t.Fatal(err)
	}
	relations = append(relations, partition)

	for _, relation := range relations {
		var dataType string
		if err := conn.QueryRow(ctx, `
			select format_type(a.atttypid, a.atttypmod)
			from pg_attribute a
			where a.attrelid = $1::regclass
			  and a.attname = 'c'
			  and not a.attisdropped`, relation).Scan(&dataType); err != nil {
			t.Fatalf("%s c type: %v", relation, err)
		}
		if dataType != "bigint" {
			t.Errorf("%s.c type = %s, want bigint", relation, dataType)
		}
	}
}

// Migration 0013 indexes events by (source, fingerprint, window) and
// includes every other column the outliers report's lookback reads, so it
// takes a short group's newest older windows with an index-only scan. Every
// partition gets it, including the default one and partitions made later.
func TestEventsSourceFingerprintWindowIndex(t *testing.T) {
	db := testdb.StartRotten(t)
	conn := db.Connect(t)
	ctx := context.Background()

	const want = "(logical_source_id, fingerprint_id, observed_window_start) INCLUDE (observed_window_end, calls, \"time\")"
	var def string
	if err := conn.QueryRow(ctx, `select pg_get_indexdef('rotten.events_source_fingerprint_window'::regclass)`).Scan(&def); err != nil {
		t.Fatalf("events_source_fingerprint_window: %v", err)
	}
	if !strings.Contains(def, "ON ONLY rotten.events USING btree "+want) && !strings.Contains(def, "ON rotten.events USING btree "+want) {
		t.Errorf("events_source_fingerprint_window = %s, want it on rotten.events %s", def, want)
	}

	if _, err := conn.Exec(ctx, `select public.create_partition_time('rotten.events', array[now() + interval '400 days'])`); err != nil {
		t.Fatalf("create a later partition: %v", err)
	}
	rows, err := conn.Query(ctx, `
		select (c.oid::regclass)::text,
		       exists (
		         select from pg_index x
		         join pg_inherits ii on ii.inhrelid = x.indexrelid
		         where x.indrelid = c.oid
		           and ii.inhparent = 'rotten.events_source_fingerprint_window'::regclass)
		from pg_inherits i
		join pg_class c on c.oid = i.inhrelid
		where i.inhparent = 'rotten.events'::regclass`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	partitions := 0
	for rows.Next() {
		var name string
		var indexed bool
		if err := rows.Scan(&name, &indexed); err != nil {
			t.Fatal(err)
		}
		partitions++
		if !indexed {
			t.Errorf("partition %s has no events_source_fingerprint_window", name)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if partitions < 2 {
		t.Fatalf("rotten.events has %d partitions, want the default one and dated ones", partitions)
	}
}

// Migration 0014 drops migration 0008's events_fingerprint_window, on
// (fingerprint_id, observed_window_start): every report that read it reads
// events_source_fingerprint_window instead (docs/perf.md). Dropping the
// partitioned index drops it from every partition, and events_source_
// fingerprint_window stays.
func TestEventsFingerprintWindowDropped(t *testing.T) {
	db := testdb.StartRottenEmpty(t)
	ctx := context.Background()
	dsn := db.DSNAs(t, testdb.OwnerRole)

	through0013 := copyMigrations(t)
	for p := range through0013 {
		if path.Ext(p) == ".sql" && p > eventsSourceFingerprintWindowMigration {
			delete(through0013, p)
		}
	}
	if _, err := migrate.UpWith(ctx, dsn, through0013, migrations.Permissions); err != nil {
		t.Fatalf("migrate to 0013: %v", err)
	}
	conn := db.Connect(t)
	// A dated partition made before the drop, besides the default one, made
	// by the owner as pg_partman's maintenance would.
	if _, err := db.ConnectAs(t, testdb.OwnerRole).Exec(ctx, `select public.create_partition_time('rotten.events', array[now() + interval '400 days'])`); err != nil {
		t.Fatalf("create a later partition: %v", err)
	}
	// Every index on rotten.events or a partition of it, keyed on exactly
	// (fingerprint_id, observed_window_start), and how many carry 0013's index.
	const fingerprintIndexes = `
		select coalesce(string_agg((x.indexrelid::regclass)::text, ', ' order by (x.indexrelid::regclass)::text), ''),
		       (select count(*) from pg_index y join pg_inherits yi on yi.inhrelid = y.indexrelid
		         where yi.inhparent = 'rotten.events_source_fingerprint_window'::regclass)
		from pg_index x
		cross join lateral (
		  select array_agg(a.attname::text order by k.ord) as cols
		  from unnest(x.indkey::int2[]) with ordinality k(attnum, ord)
		  join pg_attribute a on a.attrelid = x.indrelid and a.attnum = k.attnum) c
		where (x.indrelid = 'rotten.events'::regclass
		       or x.indrelid in (select inhrelid from pg_inherits where inhparent = 'rotten.events'::regclass))
		  and c.cols = array['fingerprint_id', 'observed_window_start']`
	var before string
	var sourceIndexesBefore int
	if err := conn.QueryRow(ctx, fingerprintIndexes).Scan(&before, &sourceIndexesBefore); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(before, "events_fingerprint_window") || strings.Count(before, ",") < 2 {
		t.Fatalf("before 0014, fingerprint indexes = %q; want 0008's on rotten.events and on its partitions", before)
	}

	if _, err := migrate.Up(ctx, dsn); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	var after string
	var sourceIndexesAfter int
	if err := conn.QueryRow(ctx, fingerprintIndexes).Scan(&after, &sourceIndexesAfter); err != nil {
		t.Fatal(err)
	}
	if after != "" {
		t.Errorf("after migrate, indexes on rotten.events (fingerprint_id, observed_window_start) = %s; want none", after)
	}
	if sourceIndexesAfter != sourceIndexesBefore || sourceIndexesAfter < 2 {
		t.Errorf("partitions with events_source_fingerprint_window: %d before, %d after; want the same, at least 2", sourceIndexesBefore, sourceIndexesAfter)
	}
}

const eventsSourceFingerprintWindowMigration = "0013_events_source_fingerprint_window.sql"

const eventContextUtilizationMigration = "0011_event_context_utilization.sql"

// Migration 0011 copies each context's logical source and share of its
// event's time onto event_context, so replica utilization doesn't join
// events. Rows from before the migration are backfilled with the same values
// the report used to compute: event time * c / the sum of the event's c.
func TestEventContextUtilizationBackfill(t *testing.T) {
	db := testdb.StartRottenEmpty(t)
	ctx := context.Background()
	dsn := db.DSNAs(t, testdb.OwnerRole)

	before := migrationsBefore(t, eventContextUtilizationMigration)
	if _, err := migrate.UpWith(ctx, dsn, before, migrations.Permissions); err != nil {
		t.Fatalf("migrate to 0010: %v", err)
	}
	conn := db.Connect(t)
	for _, q := range []string{
		`insert into rotten.logical_sources (id, project, environment, cluster, role) values
			(1, 'canvas', 'production', '13', 'primary'), (2, 'canvas', 'production', '13', 'replica')`,
		`insert into rotten.physical_sources (id, fqdn) values (1, 'p.example'), (2, 'r.example')`,
		`insert into rotten.fingerprints (id, fingerprint, normalized) values (1, 'select 1', 'select $1')`,
		`insert into rotten.controllers (id, controller) values (1, 'users')`,
		`insert into rotten.actions (id, action) values (1, 'show'), (2, 'new')`,
		`insert into rotten.job_tags (id, job_tag) values (1, 'Job#perform')`,
		// Three events: one with three contexts, one on the replica, and one
		// a day earlier, in another partition.
		`insert into rotten.events (id, fingerprint_id, logical_source_id, physical_source_id,
			observed_window_start, observed_window_end, calls, time) values
			(101, 1, 1, 1, date_trunc('hour', now()), date_trunc('hour', now()) + interval '5 minutes', 491, 250),
			(102, 1, 2, 2, date_trunc('hour', now()), date_trunc('hour', now()) + interval '5 minutes', 3, 10),
			(103, 1, 1, 1, date_trunc('hour', now()) - interval '1 day', date_trunc('hour', now()) - interval '1 day' + interval '5 minutes', 3, 7)`,
		`insert into rotten.event_context (event_id, observed_window_start, observed_window_end, controller_id, action_id, job_tag_id, c)
			select e.id, e.observed_window_start, e.observed_window_end, x.controller_id, x.action_id, x.job_tag_id, x.c
			from rotten.events e
			join (values (101, 1, 1, null::int, 200), (101, 1, 2, null, 41), (101, null, null, null, 250),
			             (102, 1, 1, null, 3),
			             (103, null, null, 1, 1), (103, null, null, 1, 2)) x(event_id, controller_id, action_id, job_tag_id, c)
			  on x.event_id = e.id`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	// What replica utilization computed before the migration.
	if _, err := conn.Exec(ctx, `create table public.expected_utilization as
		select ec.event_id, ec.c, e.logical_source_id,
			e.time * ec.c::double precision / sum(ec.c) over (partition by ec.event_id) as attributed_time
		from rotten.event_context ec
		join rotten.events e on e.id = ec.event_id and e.observed_window_start = ec.observed_window_start`); err != nil {
		t.Fatal(err)
	}

	if applied, err := migrate.Up(ctx, dsn); err != nil || !slices.Contains(applied, 11) {
		t.Fatalf("migrate to 0011: applied %v, err %v; want it to include 11", applied, err)
	}

	var rows, matched, nulls int
	if err := conn.QueryRow(ctx, `
		select count(*),
			count(*) filter (where ec.logical_source_id = x.logical_source_id and ec.attributed_time = x.attributed_time),
			count(*) filter (where ec.logical_source_id is null or ec.attributed_time is null)
		from rotten.event_context ec
		join public.expected_utilization x on x.event_id = ec.event_id and x.c = ec.c`).Scan(&rows, &matched, &nulls); err != nil {
		t.Fatal(err)
	}
	if rows != 6 || matched != 6 || nulls != 0 {
		t.Errorf("backfilled contexts: %d rows, %d match the old computation, %d null; want 6, 6, 0", rows, matched, nulls)
	}
	var usersShow float64
	if err := conn.QueryRow(ctx, `select attributed_time from rotten.event_context where event_id = 101 and c = 200`).Scan(&usersShow); err != nil {
		t.Fatal(err)
	}
	if want := 250.0 * 200.0 / 491.0; usersShow != want {
		t.Errorf("users#show attributed_time = %v, want %v", usersShow, want)
	}

	for _, relation := range []string{"rotten.event_context", "rotten.event_context_partition_template"} {
		for column, want := range map[string]string{"logical_source_id": "integer", "attributed_time": "double precision"} {
			var got string
			if err := conn.QueryRow(ctx, `
				select format_type(a.atttypid, a.atttypmod)
				from pg_attribute a
				where a.attrelid = $1::regclass and a.attname = $2 and not a.attisdropped`, relation, column).Scan(&got); err != nil {
				t.Fatalf("%s.%s: %v", relation, column, err)
			}
			if got != want {
				t.Errorf("%s.%s type = %s, want %s", relation, column, got, want)
			}
		}
	}
	var indexDef string
	if err := conn.QueryRow(ctx, `select pg_get_indexdef('rotten.event_context_source_window'::regclass)`).Scan(&indexDef); err != nil {
		t.Fatalf("event_context_source_window index: %v", err)
	}
	if !strings.Contains(indexDef, "(logical_source_id, observed_window_start)") {
		t.Errorf("event_context_source_window = %s, want it on (logical_source_id, observed_window_start)", indexDef)
	}
}

// A server from before 0011 keeps inserting context rows without
// logical_source_id and attributed_time until it's restarted.
// repair_context_utilization fills them in, a batch of events at a time, the
// same way the backfill does, and leaves rows that already have values alone.
func TestRepairContextUtilization(t *testing.T) {
	db := testdb.StartRotten(t)
	ctx := context.Background()
	conn := db.Connect(t)
	for _, q := range []string{
		`insert into rotten.logical_sources (id, project, environment, cluster, role) values
			(1, 'canvas', 'production', '13', 'primary'), (2, 'canvas', 'production', '13', 'replica')`,
		`insert into rotten.physical_sources (id, fqdn) values (1, 'p.example'), (2, 'r.example')`,
		`insert into rotten.fingerprints (id, fingerprint, normalized) values (1, 'select 1', 'select $1')`,
		`insert into rotten.controllers (id, controller) values (1, 'users')`,
		`insert into rotten.actions (id, action) values (1, 'show'), (2, 'new')`,
		`insert into rotten.events (id, fingerprint_id, logical_source_id, physical_source_id,
			observed_window_start, observed_window_end, calls, time) values
			(101, 1, 1, 1, date_trunc('hour', now()), date_trunc('hour', now()) + interval '5 minutes', 491, 250),
			(102, 1, 2, 2, date_trunc('hour', now()), date_trunc('hour', now()) + interval '5 minutes', 3, 10),
			(103, 1, 2, 2, date_trunc('hour', now()) - interval '1 day', date_trunc('hour', now()) - interval '1 day' + interval '5 minutes', 3, 7),
			(104, 1, 2, 2, date_trunc('hour', now()), date_trunc('hour', now()) + interval '5 minutes', 5, 1)`,
		// How a pre-0011 server inserts: one row at a time, naming only the old columns.
		`insert into rotten.event_context (event_id, observed_window_start, observed_window_end, controller_id, action_id, c)
			select e.id, e.observed_window_start, e.observed_window_end, x.controller_id, x.action_id, x.c
			from rotten.events e
			join (values (101, 1, 1, 200), (101, 1, 2, 41), (101, null, null, 250),
			             (102, 1, 1, 3),
			             (103, 1, 2, 1), (103, 1, 1, 2)) x(event_id, controller_id, action_id, c)
			  on x.event_id = e.id`,
		// Written by a current server; the repair must not touch it.
		`insert into rotten.event_context (event_id, observed_window_start, observed_window_end, controller_id, action_id, c,
			logical_source_id, attributed_time)
			select id, observed_window_start, observed_window_end, 1, 1, 5, 2, 123.5 from rotten.events where id = 104`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	if _, err := conn.Exec(ctx, `create table public.expected_utilization as
		select ec.event_id, ec.c, e.logical_source_id,
			e.time * ec.c::double precision / sum(ec.c) over (partition by ec.event_id) as attributed_time
		from rotten.event_context ec
		join rotten.events e on e.id = ec.event_id and e.observed_window_start = ec.observed_window_start
		where ec.event_id <> 104`); err != nil {
		t.Fatal(err)
	}

	ingest := db.ConnectAs(t, testdb.IngestRole)
	repair := func(batch int) int64 {
		t.Helper()
		var n int64
		if err := ingest.QueryRow(ctx, "select rotten.repair_context_utilization($1)", batch).Scan(&n); err != nil {
			t.Fatalf("repair_context_utilization(%d): %v", batch, err)
		}
		return n
	}
	nulls := func() int {
		t.Helper()
		var n int
		if err := conn.QueryRow(ctx, `select count(*) from rotten.event_context
			where logical_source_id is null or attributed_time is null`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	first := repair(1)
	if first < 1 || first > 3 || nulls() != 6-int(first) {
		t.Errorf("first batch of one event repaired %d rows, leaving %d null; want one event's rows (1-3) and the rest null", first, nulls())
	}
	total := first
	for i := 0; i < 5; i++ {
		total += repair(1)
	}
	if total != 6 || nulls() != 0 {
		t.Errorf("repaired %d rows, %d still null; want 6 and 0", total, nulls())
	}
	if n := repair(1000); n != 0 {
		t.Errorf("repair with nothing to do = %d, want 0", n)
	}

	var rows, matched int
	if err := conn.QueryRow(ctx, `
		select count(*),
			count(*) filter (where ec.logical_source_id = x.logical_source_id and ec.attributed_time = x.attributed_time)
		from rotten.event_context ec
		join public.expected_utilization x on x.event_id = ec.event_id and x.c = ec.c`).Scan(&rows, &matched); err != nil {
		t.Fatal(err)
	}
	if rows != 6 || matched != 6 {
		t.Errorf("repaired contexts: %d rows, %d match the old computation; want 6, 6", rows, matched)
	}
	var untouched float64
	if err := conn.QueryRow(ctx, `select attributed_time from rotten.event_context where event_id = 104`).Scan(&untouched); err != nil {
		t.Fatal(err)
	}
	if untouched != 123.5 {
		t.Errorf("already-filled context attributed_time = %v, want 123.5 untouched", untouched)
	}

	var indexDef string
	if err := conn.QueryRow(ctx, `select pg_get_indexdef('rotten.event_context_utilization_missing'::regclass)`).Scan(&indexDef); err != nil {
		t.Fatalf("event_context_utilization_missing index: %v", err)
	}
	if !strings.Contains(indexDef, "WHERE (attributed_time IS NULL)") {
		t.Errorf("event_context_utilization_missing = %s, want it partial on attributed_time IS NULL", indexDef)
	}
	for _, role := range []string{testdb.UIRole, testdb.ReadonlyRole} {
		var ok bool
		if err := conn.QueryRow(ctx, "select has_function_privilege($1, 'rotten.repair_context_utilization(integer)', 'execute')", role).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Errorf("%s can execute repair_context_utilization", role)
		}
	}
}

const fingerprintsUnparsedMigration = "0012_fingerprints_unparsed.sql"

// A worker that sends fallback fingerprints to a server from before 0012
// gets them stored as plain fingerprints. 0012 marks those unparsed by their
// prefix, which no pg_query fingerprint has.
func TestFingerprintsUnparsedBackfill(t *testing.T) {
	db := testdb.StartRottenEmpty(t)
	ctx := context.Background()
	dsn := db.DSNAs(t, testdb.OwnerRole)

	before := migrationsBefore(t, fingerprintsUnparsedMigration)
	if _, err := migrate.UpWith(ctx, dsn, before, migrations.Permissions); err != nil {
		t.Fatalf("migrate to 0011: %v", err)
	}
	conn := db.Connect(t)
	if _, err := conn.Exec(ctx, `insert into rotten.fingerprints (fingerprint, normalized) values
		('unparsed-0123456789abcdef', 'update t set x = $1 returning with (old as o) o.x'),
		('0123456789abcdef', 'select $1'),
		('xunparsed-0123456789abcdef', 'select $2')`); err != nil {
		t.Fatal(err)
	}
	if applied, err := migrate.Up(ctx, dsn); err != nil || !slices.Contains(applied, 12) {
		t.Fatalf("migrate to 0012: applied %v, err %v; want it to include 12", applied, err)
	}
	for fp, want := range map[string]bool{
		"unparsed-0123456789abcdef":  true,
		"0123456789abcdef":           false,
		"xunparsed-0123456789abcdef": false,
	} {
		var got bool
		if err := conn.QueryRow(ctx, "select unparsed from rotten.fingerprints where fingerprint = $1", fp).Scan(&got); err != nil {
			t.Fatalf("%s: %v", fp, err)
		}
		if got != want {
			t.Errorf("%s unparsed = %v, want %v", fp, got, want)
		}
	}
}
