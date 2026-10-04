package migrate_test

import (
	"context"
	"errors"
	"strings"
	"testing"

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
		"fingerprint": "Hex string from fingerprint.Normalized, matching proto.rotten.v1.FingerprintAggregate.fingerprint.",
		"normalized":  "pg_query.Normalize output of one representative query text for this fingerprint, stored only on first insert.",
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
		"id":              "bigint",
		"email":           "text",
		"name":            "text",
		"provider":        "text",
		"provider_uid":    "text",
		"password_digest": "text",
		"role":            "text",
		"groups":          "text[]",
		"active":          "boolean",
		"last_login_at":   "timestamp with time zone",
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

	before := copyMigrations(t)
	if _, ok := before[usersProviderUIDMigration]; !ok {
		t.Fatalf("migration %s not found", usersProviderUIDMigration)
	}
	delete(before, usersProviderUIDMigration)
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
	if applied, err := migrate.Up(ctx, dsn); err != nil || len(applied) != 1 || applied[0] != 9 {
		t.Fatalf("migrate after fixing duplicates: applied %v, err %v; want [9]", applied, err)
	}
}

const usersProviderUIDMigration = "0009_users_provider_uid_unique.sql"

func TestUsersColumnCommentsDescribeValues(t *testing.T) {
	db := testdb.StartRotten(t)
	conn := db.Connect(t)
	ctx := context.Background()

	want := map[string]string{
		"id":              "Primary key for UI users.",
		"email":           "Email address used to identify the user; the UI stores it lowercased, and uniqueness is case-insensitive.",
		"name":            "Display name from the identity provider or password admin.",
		"provider":        "Authentication provider name for externally authenticated users.",
		"provider_uid":    "Provider-specific stable user identifier.",
		"password_digest": "bcrypt password digest for password auth; null for OIDC users.",
		"role":            "Authorization role: viewer or admin.",
		"groups":          "External identity provider groups observed at last login.",
		"active":          "Local kill switch; inactive users are logged out on their next request.",
		"last_login_at":   "Time this user last completed authentication.",
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
