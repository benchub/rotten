package testdb

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func isDenied(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42501"
}

// loadObserver runs schema/observer.sql with the given role and schema names.
func loadObserver(t *testing.T, db *DB, role, schema string) (string, error) {
	t.Helper()
	path := filepath.Join(RepoRoot(), "schema", "observer.sql")
	return db.PSQL(t, path, map[string]string{"observer_role": role, "observer_schema": schema})
}

// execAll runs each statement on conn and fails the test on the first error.
func execAll(t *testing.T, conn *pgx.Conn, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := conn.Exec(context.Background(), s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
}

// TestObserverWrapperHijack checks that a role with CREATE in the
// pg_stat_statements schema can't hijack the min/max reset wrapper (which runs
// as its superuser owner) by adding a better-matching overload, and that only
// the observer may call the wrapper.
func TestObserverWrapperHijack(t *testing.T) {
	for _, v := range []int{17, 18} {
		t.Run(fmt.Sprintf("pg%d", v), func(t *testing.T) {
			t.Parallel()
			db := StartObserved(t, v)
			su := db.Connect(t)
			ctx := context.Background()
			const observer, schema = "obs", "rotten"

			execAll(t, su,
				"create role evil login password '"+db.RolePassword("evil")+"'",
				"create role app login password '"+db.RolePassword("app")+"'",
				"grant create on schema public to evil",
				"create table public.hijacked (who text)",
				"grant insert on public.hijacked to public")

			evil, err := pgx.Connect(ctx, db.DSNAs(t, "evil"))
			if err != nil {
				t.Fatal(err)
			}
			defer evil.Close(ctx)
			overload := `create function public.pg_stat_statements_reset(int, int, int, bool)
				returns timestamptz language sql as
				$$ insert into public.hijacked values (current_user); select now() $$`

			// The overload exists when the script creates the wrapper, and is
			// recreated afterwards too.
			execAll(t, evil, overload)
			if out, err := loadObserver(t, db, observer, schema); err != nil {
				t.Fatalf("observer.sql: %v\n%s", err, out)
			}
			execAll(t, evil, "drop function public.pg_stat_statements_reset(int, int, int, bool)", overload)
			execAll(t, su, fmt.Sprintf("alter role %s password '%s'", observer, db.RolePassword(observer)))

			execAll(t, su, "select 1 as rotten_marker")
			const q = `select minmax_stats_since from pg_stat_statements where query like '%rotten_marker%'`
			var before, after time.Time
			if err := su.QueryRow(ctx, q).Scan(&before); err != nil {
				t.Fatal(err)
			}

			obs, err := pgx.Connect(ctx, db.DSNAs(t, observer))
			if err != nil {
				t.Fatal(err)
			}
			defer obs.Close(ctx)
			if _, err := obs.Exec(ctx, "select rotten.pg_stat_statements_minmax_reset()"); err != nil {
				t.Errorf("wrapper: %v", err)
			}
			var n int
			if err := su.QueryRow(ctx, "select count(*) from public.hijacked").Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n != 0 {
				t.Errorf("wrapper ran the hijacking overload %d time(s)", n)
			}
			if err := su.QueryRow(ctx, q).Scan(&after); err != nil {
				t.Fatal(err)
			}
			if !after.After(before) {
				t.Errorf("minmax_stats_since = %v, want after %v: real reset didn't run", after, before)
			}

			// A non-observer with USAGE on the schema still can't call it.
			execAll(t, su, "grant usage on schema rotten to app")
			app, err := pgx.Connect(ctx, db.DSNAs(t, "app"))
			if err != nil {
				t.Fatal(err)
			}
			defer app.Close(ctx)
			if _, err := app.Exec(ctx, "select rotten.pg_stat_statements_minmax_reset()"); !isDenied(err) {
				t.Errorf("non-observer calling wrapper: err = %v, want 42501", err)
			}
		})
	}
}

// TestObserverRejectsUnsafeSchema checks the script refuses a wrapper schema
// someone else owns, or one where another role has CREATE.
func TestObserverRejectsUnsafeSchema(t *testing.T) {
	db := StartObserved(t, 17)
	su := db.Connect(t)
	execAll(t, su,
		"create role app",
		"create schema other_owned authorization app",
		"create schema shared_create",
		"grant create on schema shared_create to app")
	for s, want := range map[string]string{
		"other_owned":   "is owned by app",
		"shared_create": "role app has CREATE on schema shared_create",
	} {
		out, err := loadObserver(t, db, "obs", s)
		if err == nil || !strings.Contains(out, want) {
			t.Errorf("observer.sql on unsafe schema %s: err = %v, want output containing %q:\n%s", s, err, want, out)
		}
		var n int
		if err := su.QueryRow(context.Background(),
			"select count(*) from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = $1", s).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("wrapper created in unsafe schema %s", s)
		}
	}

	// A wrapper someone else already owns: CREATE OR REPLACE would keep that
	// owner, so the script must refuse it.
	execAll(t, su,
		"create schema preowned",
		"grant create on schema preowned to app",
		"set role app",
		"create function preowned.pg_stat_statements_minmax_reset() returns timestamptz language sql as 'select now()'",
		"reset role",
		"revoke create on schema preowned from app")
	out, err := loadObserver(t, db, "obs", "preowned")
	if want := "is owned by app"; err == nil || !strings.Contains(out, want) {
		t.Errorf("observer.sql on preowned wrapper: err = %v, want output containing %q:\n%s", err, want, out)
	}
	var owner, acl string
	if err := su.QueryRow(context.Background(),
		"select proowner::regrole::text, coalesce(proacl::text, '') from pg_proc where oid = 'preowned.pg_stat_statements_minmax_reset()'::regprocedure").
		Scan(&owner, &acl); err != nil {
		t.Fatal(err)
	}
	if owner != "app" {
		t.Errorf("preowned wrapper owner = %s, want app", owner)
	}
	if strings.Contains(acl, "obs=") {
		t.Errorf("preowned wrapper ACL %s grants to the observer", acl)
	}
}

// TestObserverOldExtension checks that on 17+ with pg_stat_statements still at
// 1.10 (as after a pg_upgrade), the script refuses with a clear message that
// says to update the extension, changes nothing, and works once it's updated.
func TestObserverOldExtension(t *testing.T) {
	for _, v := range []int{17, 18} {
		t.Run(fmt.Sprintf("pg%d", v), func(t *testing.T) {
			t.Parallel()
			db := StartObserved(t, v)
			su := db.Connect(t)
			ctx := context.Background()
			const observer, schema = "obs", "rotten"

			execAll(t, su,
				"drop extension pg_stat_statements",
				"create extension pg_stat_statements version '1.10'")

			path := filepath.Join(RepoRoot(), "schema", "observer.sql")
			out, err := db.PSQL(t, path, map[string]string{
				"observer_role": observer, "observer_schema": schema, "VERBOSITY": "verbose"})
			const want = "55000: pg_stat_statements is at version 1.10, but Postgres 17 and later need 1.11 or newer; run ALTER EXTENSION pg_stat_statements UPDATE"
			if err == nil || !strings.Contains(out, want) {
				t.Fatalf("observer.sql on extension 1.10: err = %v, want output containing %q:\n%s", err, want, out)
			}
			var roles, schemas int
			if err := su.QueryRow(ctx, "select count(*) from pg_roles where rolname = $1", observer).Scan(&roles); err != nil {
				t.Fatal(err)
			}
			if err := su.QueryRow(ctx, "select count(*) from pg_namespace where nspname = $1", schema).Scan(&schemas); err != nil {
				t.Fatal(err)
			}
			if roles != 0 || schemas != 0 {
				t.Errorf("failed run left %d role(s) and %d schema(s) behind, want none", roles, schemas)
			}

			execAll(t, su, "alter extension pg_stat_statements update")
			if out, err := loadObserver(t, db, observer, schema); err != nil {
				t.Fatalf("observer.sql after update: %v\n%s", err, out)
			}
			execAll(t, su, fmt.Sprintf("alter role %s password '%s'", observer, db.RolePassword(observer)))
			obs, err := pgx.Connect(ctx, db.DSNAs(t, observer))
			if err != nil {
				t.Fatal(err)
			}
			defer obs.Close(ctx)
			if _, err := obs.Exec(ctx, "select rotten.pg_stat_statements_minmax_reset()"); err != nil {
				t.Errorf("wrapper after update: %v", err)
			}
		})
	}
}

// TestObserverSQL loads schema/observer.sql into each observed version and
// checks what the observer role can and can't do.
func TestObserverSQL(t *testing.T) {
	for _, v := range ObservedVersions {
		t.Run(fmt.Sprintf("pg%d", v), func(t *testing.T) {
			t.Parallel()
			db := StartObserved(t, v)
			su := db.Connect(t)
			ctx := context.Background()
			const observer = "rotten_observer_test"
			const schema = "rotten_obs_schema"

			path := filepath.Join(RepoRoot(), "schema", "observer.sql")
			vars := map[string]string{"observer_role": observer, "observer_schema": schema}
			// Run it twice: the script must be safe to rerun.
			for i := 0; i < 2; i++ {
				if out, err := db.PSQL(t, path, vars); err != nil {
					t.Fatalf("run %d of observer.sql: %v\n%s", i+1, err, out)
				}
			}
			// The script doesn't set a password. Give it one so we can log in.
			if _, err := su.Exec(ctx, fmt.Sprintf("alter role %s password '%s'", pgx.Identifier{observer}.Sanitize(), db.RolePassword(observer))); err != nil {
				t.Fatal(err)
			}

			// Another role runs a query with recognizable text.
			if _, err := su.Exec(ctx, "create role app login password '"+db.RolePassword("app")+"'"); err != nil {
				t.Fatal(err)
			}
			app, err := pgx.Connect(ctx, db.DSNAs(t, "app"))
			if err != nil {
				t.Fatal(err)
			}
			defer app.Close(ctx)
			if _, err := app.Exec(ctx, "select 1 as rotten_marker"); err != nil {
				t.Fatal(err)
			}

			obs, err := pgx.Connect(ctx, db.DSNAs(t, observer))
			if err != nil {
				t.Fatalf("connect as observer: %v", err)
			}
			defer obs.Close(ctx)

			// Without pg_read_all_stats, another role's rows still show up, but
			// with query = '<insufficient privilege>'.
			rows, err := obs.Query(ctx, `select s.query from pg_stat_statements s
				join pg_roles r on r.oid = s.userid where r.rolname = 'app'`)
			if err != nil {
				t.Fatal(err)
			}
			texts, err := pgx.CollectRows(rows, pgx.RowTo[string])
			if err != nil {
				t.Fatal(err)
			}
			if len(texts) == 0 {
				t.Fatal("no pg_stat_statements rows for app")
			}
			if !strings.Contains(strings.Join(texts, "\n"), "rotten_marker") {
				t.Errorf("observer can't see app's query text; got %q", texts)
			}

			const appStats = `select s.calls, s.minmax_stats_since from pg_stat_statements s
				join pg_roles r on r.oid = s.userid
				where r.rolname = 'app' and s.query like '%rotten_marker%'`
			if v >= 17 {
				var calls0, calls1 int64
				var since0, since1 time.Time
				if err := obs.QueryRow(ctx, appStats).Scan(&calls0, &since0); err != nil {
					t.Fatal(err)
				}
				if _, err := obs.Exec(ctx, "select "+schema+".pg_stat_statements_minmax_reset()"); err != nil {
					t.Errorf("min/max-only reset via wrapper: %v", err)
				}
				if err := obs.QueryRow(ctx, appStats).Scan(&calls1, &since1); err != nil {
					t.Fatal(err)
				}
				if !since1.After(since0) {
					t.Errorf("minmax_stats_since = %v after reset, want after %v", since1, since0)
				}
				if calls1 != calls0 {
					t.Errorf("calls = %d after min/max reset, want unchanged %d", calls1, calls0)
				}
				// The raw four-argument function stays ungranted.
				if _, err := obs.Exec(ctx, "select pg_stat_statements_reset(0, 0, 0, true)"); !isDenied(err) {
					t.Errorf("raw min/max reset as observer: err = %v, want permission denied", err)
				}
			} else {
				var n int
				if err := su.QueryRow(ctx, "select count(*) from pg_proc where proname = 'pg_stat_statements_minmax_reset'").Scan(&n); err != nil || n != 0 {
					t.Errorf("wrapper exists on %d (count %d, err %v), want none", v, n, err)
				}
			}

			for _, q := range []string{
				"select pg_stat_statements_reset()",
				"select pg_stat_statements_reset(0, 0, 0)",
			} {
				if _, err := obs.Exec(ctx, q); !isDenied(err) {
					t.Errorf("%s as observer: err = %v, want permission denied (42501)", q, err)
				}
			}
		})
	}
}
