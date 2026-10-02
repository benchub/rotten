package migrate_test

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/benchub/rotten/internal/migrate"
	"github.com/benchub/rotten/internal/testdb"
	"github.com/benchub/rotten/migrations"
)

// seed inserts the rows the role tests need, as the superuser.
func seed(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	ctx := context.Background()
	for _, q := range []string{
		"insert into rotten.fingerprints (id, fingerprint, normalized) values (1, 'q', 'q')",
		"insert into rotten.physical_sources (id, fqdn) values (1, 'h')",
		// The first key gets id 1, which the tests rely on.
		"insert into rotten.api_keys (name, secret_hash, created_by) values ('k', 'hash', 'admin')",
		"insert into rotten.ingested_batches (batch_id, key_id) values ('b0', 1)",
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
}

const insertEvent = `insert into rotten.events (fingerprint_id, logical_source_id, physical_source_id,
	observed_window_start, observed_window_end, calls, time)
	values (1, 0, 1, now(), now() + interval '1 minute', 1, 1)`

// wantDenied fails unless q fails with insufficient_privilege (42501).
func wantDenied(t *testing.T, conn *pgx.Conn, q string) {
	t.Helper()
	_, err := conn.Exec(context.Background(), q)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Errorf("%s: got err %v, want permission denied (42501)", q, err)
	}
}

func wantAllowed(t *testing.T, conn *pgx.Conn, q string) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), q); err != nil {
		t.Errorf("%s: %v", q, err)
	}
}

// userTables lists every ordinary or partitioned table in rotten, not
// counting partitions.
func userTables(t *testing.T, conn *pgx.Conn) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), `
		select 'rotten.' || quote_ident(c.relname) from pg_class c
		join pg_namespace n on n.oid = c.relnamespace
		where n.nspname = 'rotten' and c.relkind in ('r', 'p') and not c.relispartition
		order by 1`)
	if err != nil {
		t.Fatal(err)
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	if len(names) < 10 {
		t.Fatalf("found only %d tables in rotten: %v", len(names), names)
	}
	return names
}

// firstColumn returns the first column of tbl, for a no-op update.
func firstColumn(t *testing.T, conn *pgx.Conn, tbl string) string {
	t.Helper()
	var col string
	if err := conn.QueryRow(context.Background(), `select attname from pg_attribute
		where attrelid = $1::regclass and attnum > 0 and not attisdropped order by attnum limit 1`, tbl).Scan(&col); err != nil {
		t.Fatal(err)
	}
	return pgx.Identifier{col}.Sanitize()
}

func TestIngestRolePermissions(t *testing.T) {
	db := testdb.StartRotten(t)
	su := db.Connect(t)
	seed(t, su)
	c := db.ConnectAs(t, testdb.IngestRole)

	wantAllowed(t, c, insertEvent)
	wantAllowed(t, c, "select count(*) from rotten.events")
	wantAllowed(t, c, "select secret_hash, revoked_at from rotten.api_keys")
	wantAllowed(t, c, "update rotten.api_keys set last_used_at = now() where id = 1")
	wantAllowed(t, c, `insert into rotten.logical_sources (project, environment, cluster, role)
		values ('all', 'all', 'all', 'all')
		on conflict (cluster, role, project, environment) do update set project = rotten.logical_sources.project`)
	wantAllowed(t, c, `insert into rotten.physical_sources (fqdn)
		values ('h')
		on conflict (fqdn) do update set fqdn = rotten.physical_sources.fqdn`)
	wantAllowed(t, c, "insert into rotten.ingested_batches (batch_id, key_id) values ('b1', 1)")
	if _, err := su.Exec(context.Background(),
		"insert into rotten.ingested_batches (batch_id, key_id, received_at) values ('old', 1, now() - interval '31 days')"); err != nil {
		t.Fatal(err)
	}
	var pruned int64
	if err := c.QueryRow(context.Background(), "select rotten.prune_ingested_batches()").Scan(&pruned); err != nil {
		t.Errorf("prune_ingested_batches: %v", err)
	}
	var left string
	if err := su.QueryRow(context.Background(), "select string_agg(batch_id, ',' order by batch_id) from rotten.ingested_batches").Scan(&left); err != nil {
		t.Fatal(err)
	}
	if pruned != 1 || left != "b0,b1" {
		t.Errorf("prune removed %d rows, left %q; want 1 removed, b0,b1 left", pruned, left)
	}
	wantAllowed(t, c, `insert into rotten.fingerprint_stats (fingerprint_id, logical_source_id, type, count)
		values (1, 0, 'calls', 1) on conflict (fingerprint_id, logical_source_id, type) do update set count = 2`)

	wantDenied(t, c, "insert into rotten.api_keys (name, secret_hash, created_by) values ('x', 'y', 'z')")
	wantDenied(t, c, "update rotten.api_keys set revoked_at = now() where id = 1")
	wantDenied(t, c, "update rotten.api_keys set revoked_by = 'me' where id = 1")
	for _, tbl := range userTables(t, su) {
		wantDenied(t, c, "delete from "+tbl)
		wantDenied(t, c, "truncate "+tbl)
	}
}

func TestUIRolePermissions(t *testing.T) {
	db := testdb.StartRotten(t)
	seed(t, db.Connect(t))
	c := db.ConnectAs(t, testdb.UIRole)

	wantDenied(t, c, insertEvent)
	wantAllowed(t, c, "select count(*) from rotten.events")
	wantAllowed(t, c, "insert into rotten.api_keys (name, secret_hash, created_by) values ('k2', 'h2', 'admin')")
	wantAllowed(t, c, "update rotten.api_keys set revoked_at = now(), revoked_by = 'admin' where id = 1")
	wantDenied(t, c, "update rotten.api_keys set last_used_at = now() where id = 1")
	wantDenied(t, c, "update rotten.api_keys set secret_hash = 'x' where id = 1")
	wantDenied(t, c, "delete from rotten.api_keys")
	wantDenied(t, c, "select * from rotten.ingested_batches")
}

func TestReadonlyRoleCantWrite(t *testing.T) {
	db := testdb.StartRotten(t)
	su := db.Connect(t)
	seed(t, su)
	c := db.ConnectAs(t, testdb.ReadonlyRole)

	wantAllowed(t, c, "select count(*) from rotten.events")
	wantAllowed(t, c, "select count(*) from rotten.fingerprint_stats")
	wantDenied(t, c, "select * from rotten.api_keys")
	wantDenied(t, c, insertEvent)
	wantDenied(t, c, "select rotten.prune_ingested_batches()")
	for _, tbl := range userTables(t, su) {
		col := firstColumn(t, su, tbl)
		// An update that matches no rows still needs the privilege.
		wantDenied(t, c, "update "+tbl+" set "+col+" = "+col+" where false")
		wantDenied(t, c, "delete from "+tbl)
		wantDenied(t, c, "truncate "+tbl)
	}
	wantDenied(t, c, "create table rotten.mine (x int)")
}

func TestNewTableUnreachableUntilListed(t *testing.T) {
	db := testdb.StartRottenEmpty(t)
	ctx := context.Background()
	dsn := db.DSNAs(t, testdb.OwnerRole)

	withExtra := copyMigrations(t)
	withExtra["9999_extra.sql"] = &fstest.MapFile{Data: []byte(
		"-- +goose Up\ncreate table rotten.extra (x int);\ninsert into rotten.extra values (1);\n-- +goose Down\ndrop table rotten.extra;\n")}

	if _, err := migrate.UpWith(ctx, dsn, withExtra, migrations.Permissions); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	for _, r := range []string{testdb.IngestRole, testdb.UIRole, testdb.ReadonlyRole} {
		c := db.ConnectAs(t, r)
		// Each role reaches the schema, so the denial is the table's own.
		wantAllowed(t, c, "select count(*) from rotten.events")
		_, err := c.Exec(ctx, "select * from rotten.extra")
		if err == nil || !strings.Contains(err.Error(), "permission denied for table extra") {
			t.Errorf("%s: select from rotten.extra: err %v, want permission denied for table extra", r, err)
		}
	}

	listed := migrations.Permissions + "\ngrant select on rotten.extra to rotten_readonly;\n"
	if _, err := migrate.UpWith(ctx, dsn, withExtra, listed); err != nil {
		t.Fatalf("migrate with extra grant: %v", err)
	}
	wantAllowed(t, db.ConnectAs(t, testdb.ReadonlyRole), "select * from rotten.extra")
	wantDenied(t, db.ConnectAs(t, testdb.IngestRole), "select * from rotten.extra")

	// Taking the grant back out takes the access away on the next run.
	if _, err := migrate.UpWith(ctx, dsn, withExtra, migrations.Permissions); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	wantDenied(t, db.ConnectAs(t, testdb.ReadonlyRole), "select * from rotten.extra")
}

func TestPermissionsFailClearlyWithoutRole(t *testing.T) {
	db := testdb.StartRottenEmpty(t)
	if _, err := db.Connect(t).Exec(context.Background(), "drop role rotten_readonly"); err != nil {
		t.Fatal(err)
	}
	_, err := migrate.Up(context.Background(), db.DSNAs(t, testdb.OwnerRole))
	if err == nil || !strings.Contains(err.Error(), `role "rotten_readonly" doesn't exist`) {
		t.Fatalf("migrate err = %v, want it to name the missing role", err)
	}
}

func TestUIRoleCantSeeSecretHash(t *testing.T) {
	db := testdb.StartRotten(t)
	seed(t, db.Connect(t))
	c := db.ConnectAs(t, testdb.UIRole)
	wantAllowed(t, c, "select id, name, fqdn, created_at, created_by, last_used_at, revoked_at, revoked_by from rotten.api_keys")
	wantDenied(t, c, "select secret_hash from rotten.api_keys")
	wantDenied(t, c, "select * from rotten.api_keys")
	wantDenied(t, c, "insert into rotten.api_keys (name, secret_hash, created_by, last_used_at) values ('k3', 'h3', 'admin', now())")
	wantDenied(t, c, "insert into rotten.api_keys (name, secret_hash, created_by, revoked_at, revoked_by) values ('k4', 'h4', 'admin', now(), 'admin')")
}

func TestUnlistedFunctionNotExecutable(t *testing.T) {
	db := testdb.StartRottenEmpty(t)
	ctx := context.Background()
	m := copyMigrations(t)
	m["9998_func.sql"] = &fstest.MapFile{Data: []byte(
		"-- +goose Up\ncreate function rotten.extra_fn() returns int language sql return 1;\n-- +goose Down\ndrop function rotten.extra_fn();\n")}
	if _, err := migrate.UpWith(ctx, db.DSNAs(t, testdb.OwnerRole), m, migrations.Permissions); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	su := db.Connect(t)
	for _, r := range []string{"public", testdb.IngestRole, testdb.UIRole, testdb.ReadonlyRole} {
		var ok bool
		if err := su.QueryRow(ctx, "select has_function_privilege($1, 'rotten.extra_fn()', 'execute')", r).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Errorf("%s can execute rotten.extra_fn()", r)
		}
	}
	// Functions rotten_owner creates later, even mid-migration before
	// permissions.sql runs, start without PUBLIC execute.
	owner := db.ConnectAs(t, testdb.OwnerRole)
	if _, err := owner.Exec(ctx, "create function rotten.later_fn() returns int language sql return 1"); err != nil {
		t.Fatal(err)
	}
	var ok bool
	if err := su.QueryRow(ctx, "select has_function_privilege('public', 'rotten.later_fn()', 'execute')").Scan(&ok); err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("PUBLIC can execute a function rotten_owner created after migrate; want the default revoked")
	}
}

func TestMigrateRemovesSchemaCreate(t *testing.T) {
	db := testdb.StartRotten(t)
	ctx := context.Background()
	su := db.Connect(t)
	roles := []string{testdb.IngestRole, testdb.UIRole, testdb.ReadonlyRole}
	for _, r := range roles {
		if _, err := su.Exec(ctx, "grant create on schema rotten to "+r); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := migrate.Up(ctx, db.DSNAs(t, testdb.OwnerRole)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	for _, r := range roles {
		var ok bool
		if err := su.QueryRow(ctx, "select has_schema_privilege($1, 'rotten', 'create')", r).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Errorf("%s still has CREATE on schema rotten after migrate", r)
		}
	}
}

func copyMigrations(t *testing.T) fstest.MapFS {
	t.Helper()
	m := fstest.MapFS{}
	err := fs.WalkDir(migrations.FS, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := fs.ReadFile(migrations.FS, p)
		m[p] = &fstest.MapFile{Data: b}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}
