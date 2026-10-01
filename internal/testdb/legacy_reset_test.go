package testdb

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestLegacyResetHijack installs pg_stat_statements outside public, plants a
// public.pg_stat_statements_reset() as an unprivileged role, and checks that
// dba.pg_stat_statements_user_reset() (which runs as its superuser owner)
// still does a real reset and never runs the planted function.
func TestLegacyResetHijack(t *testing.T) {
	type tc struct {
		v      int
		schema string
		plants []string
	}
	zeroArg := `create function public.pg_stat_statements_reset() returns void language sql as
		$$ insert into public.hijacked values (current_user) $$`
	// With the extension in public, a variadic overload with a default could
	// make an unqualified, uncast call ambiguous ("not unique"), and a ()
	// overload could win it outright.
	variadic := `create function public.pg_stat_statements_reset(variadic int[] default '{}') returns void language sql as
		$$ insert into public.hijacked values (current_user) $$`
	var cases []tc
	for _, v := range []int{14, 17} {
		cases = append(cases,
			tc{v, "pgss", []string{zeroArg}},
			tc{v, "public", []string{variadic, zeroArg}})
	}
	for _, c := range cases {
		v := c.v
		t.Run(fmt.Sprintf("pg%d/%s", v, c.schema), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db := StartObserved(t, v)
			su := db.Connect(t)
			if c.schema != "public" {
				execAll(t, su,
					"drop extension pg_stat_statements",
					"create schema "+c.schema,
					"create extension pg_stat_statements schema "+c.schema)
			}
			execAll(t, su,
				"create role evil login password 'evil'",
				"grant create on schema public to evil",
				"create table public.hijacked (who text)",
				"grant insert on public.hijacked to public")

			evil, err := pgx.Connect(ctx, db.DSNAs(t, "evil"))
			if err != nil {
				t.Fatal(err)
			}
			defer evil.Close(ctx)
			// The real function always takes three or four arguments, so the
			// planted () and variadic overloads never collide with it.
			plant := func() { execAll(t, evil, c.plants...) }
			unplant := func() {
				execAll(t, su,
					"drop function if exists public.pg_stat_statements_reset(variadic int[])",
					"drop function if exists public.pg_stat_statements_reset()")
			}
			plant()

			for _, f := range []string{"observer.sql", "legacy_reset.sql"} {
				if out, err := db.PSQL(t, filepath.Join(RepoRoot(), "schema", f), nil); err != nil {
					t.Fatalf("%s: %v\n%s", f, err, out)
				}
			}
			// Replant after install too, in case the body resolves at run time.
			unplant()
			plant()
			execAll(t, su, "alter role rotten_observer password 'rotten_observer'")

			var before, after time.Time
			q := `select stats_reset from ` + c.schema + `.pg_stat_statements_info`
			if err := su.QueryRow(ctx, q).Scan(&before); err != nil {
				t.Fatal(err)
			}
			obs, err := pgx.Connect(ctx, db.DSNAs(t, "rotten_observer"))
			if err != nil {
				t.Fatal(err)
			}
			defer obs.Close(ctx)
			if _, err := obs.Exec(ctx, "select dba.pg_stat_statements_user_reset()"); err != nil {
				t.Fatalf("reset: %v", err)
			}
			if err := su.QueryRow(ctx, q).Scan(&after); err != nil {
				t.Fatal(err)
			}
			if !after.After(before) {
				t.Errorf("stats_reset %v not after %v: no real reset", after, before)
			}
			var n int
			if err := su.QueryRow(ctx, "select count(*) from public.hijacked").Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n != 0 {
				t.Errorf("planted function ran %d times", n)
			}
			// Only the observer may call it.
			if _, err := evil.Exec(ctx, "select dba.pg_stat_statements_user_reset()"); !isDenied(err) {
				t.Errorf("evil calling reset: err = %v, want permission denied", err)
			}
		})
	}
}
