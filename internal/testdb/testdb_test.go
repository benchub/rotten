package testdb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestRottenRolesCanLogIn(t *testing.T) {
	db := StartRotten(t)
	ctx := context.Background()
	for _, r := range RottenRoles {
		conn, err := pgx.Connect(ctx, db.DSNAs(t, r))
		if err != nil {
			t.Errorf("connect as %s: %v", r, err)
			continue
		}
		var who string
		if err := conn.QueryRow(ctx, "select current_user").Scan(&who); err != nil || who != r {
			t.Errorf("current_user = %q (err %v), want %q", who, err, r)
		}
		conn.Close(ctx)
	}
}

func TestRottenSchemaLoads(t *testing.T) {
	db := StartRotten(t)
	conn := db.Connect(t)
	ctx := context.Background()

	for _, r := range RottenRoles {
		var ok bool
		if err := conn.QueryRow(ctx, "select exists(select from pg_roles where rolname = $1)", r).Scan(&ok); err != nil || !ok {
			t.Fatalf("role %s missing (err %v)", r, err)
		}
	}

	sql, err := os.ReadFile(filepath.Join(RepoRoot(), "schema", "tables.sql"))
	if err != nil {
		t.Fatal(err)
	}
	// Exec without arguments uses the simple protocol, so the whole file runs
	// as one multi-statement batch.
	if _, err := conn.Exec(ctx, string(sql)); err != nil {
		t.Fatalf("load schema/tables.sql: %v", err)
	}

	for _, parent := range []string{"rotten.events", "rotten.event_context"} {
		var name string
		var exists bool
		err := conn.QueryRow(ctx,
			"select partition_table, table_exists from public.show_partition_name($1, now()::text)", parent).
			Scan(&name, &exists)
		if err != nil {
			t.Fatalf("show_partition_name(%s): %v", parent, err)
		}
		if !exists {
			t.Errorf("%s: today's partition %s doesn't exist", parent, name)
		}
	}
}

func TestStartObserved(t *testing.T) {
	for _, v := range ObservedVersions {
		t.Run(fmt.Sprintf("pg%d", v), func(t *testing.T) {
			t.Parallel()
			db := StartObserved(t, v)
			conn := db.Connect(t)
			ctx := context.Background()

			var num int
			if err := conn.QueryRow(ctx, "select current_setting('server_version_num')::int").Scan(&num); err != nil {
				t.Fatal(err)
			}
			if num/10000 != v {
				t.Errorf("server_version_num = %d, want major %d", num, v)
			}
			var tp string
			if err := conn.QueryRow(ctx, "show pg_stat_statements.track_planning").Scan(&tp); err != nil {
				t.Fatal(err)
			}
			if tp != "on" {
				t.Errorf("track_planning = %q, want on", tp)
			}
			var n int64
			if err := conn.QueryRow(ctx, "select count(*) from pg_class where relname = 'rotten_probe'").Scan(&n); err != nil {
				t.Fatal(err)
			}
			// plans > 0 relies on pgx's default extended protocol planning the probe.
			var calls, plans int64
			err := conn.QueryRow(ctx, `select calls, plans from pg_stat_statements
				where query = 'select count(*) from pg_class where relname = $1'`).Scan(&calls, &plans)
			if err != nil {
				t.Fatalf("probe query not in pg_stat_statements: %v", err)
			}
			if calls < 1 || plans < 1 {
				t.Errorf("probe calls = %d, plans = %d; want both > 0", calls, plans)
			}
		})
	}
}
