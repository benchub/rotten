package migrate_test

import (
	"context"
	"strings"
	"testing"

	"github.com/benchub/rotten/internal/migrate"
	"github.com/benchub/rotten/internal/testdb"
)

func TestParseRetention(t *testing.T) {
	good := map[string]migrate.Retention{
		"21 days": 21, "1 day": 1, " 30  DAYS ": 30, "7d": 7, "90d": 90,
		"504h": 21, "48h0m0s": 2, "3650 days": 3650,
	}
	for in, want := range good {
		got, err := migrate.ParseRetention(in)
		if err != nil {
			t.Errorf("ParseRetention(%q): %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("ParseRetention(%q) = %d, want %d", in, got, want)
		}
	}
	bad := []string{"", "0d", "0 days", "-3d", "-3 days", "3651 days", "1 month",
		"21", "21 days'; drop table rotten.events; --", "12h", "25h", "abc", "1.5d",
		"99999999999999999999d", "-24h", "1 week"}
	for _, in := range bad {
		if r, err := migrate.ParseRetention(in); err == nil {
			t.Errorf("ParseRetention(%q) = %d, want an error", in, r)
		}
	}
}

func retentions(t *testing.T, db *testdb.DB) map[string]string {
	t.Helper()
	rows, err := db.Connect(t).Query(context.Background(),
		`select parent_table, retention from public.part_config
		 where parent_table in ('rotten.events', 'rotten.event_context')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var p, r string
		if err := rows.Scan(&p, &r); err != nil {
			t.Fatal(err)
		}
		got[p] = r
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("part_config rows = %v, want both tables", got)
	}
	return got
}

func wantRetention(t *testing.T, db *testdb.DB, want string) {
	t.Helper()
	for p, r := range retentions(t, db) {
		if r != want {
			t.Errorf("%s retention = %q, want %q", p, r, want)
		}
	}
}

func TestMigrateRetention(t *testing.T) {
	db := testdb.StartRottenEmpty(t)
	ctx := context.Background()
	dsn := db.DSNAs(t, testdb.OwnerRole)

	if _, err := migrate.Up(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	wantRetention(t, db, "21 days")

	r, err := migrate.ParseRetention("45d")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrate.UpRetention(ctx, dsn, r); err != nil {
		t.Fatal(err)
	}
	wantRetention(t, db, "45 days")

	// Up reapplies the default on every run, like permissions.sql.
	if _, err := migrate.Up(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	wantRetention(t, db, "21 days")

	// An out-of-range Retention that skipped ParseRetention fails and
	// changes nothing.
	if _, err := migrate.UpRetention(ctx, dsn, migrate.Retention(0)); err == nil ||
		!strings.Contains(err.Error(), "retention") {
		t.Errorf("UpRetention(0) err = %v, want a retention error", err)
	}
	wantRetention(t, db, "21 days")
}

// An invalid Retention fails before goose runs, so an empty database stays
// empty.
func TestMigrateInvalidRetentionChangesNothing(t *testing.T) {
	db := testdb.StartRottenEmpty(t)
	ctx := context.Background()
	if _, err := migrate.UpRetention(ctx, db.DSNAs(t, testdb.OwnerRole), migrate.Retention(-1)); err == nil {
		t.Fatal("UpRetention(-1) succeeded")
	}
	var n int
	if err := db.Connect(t).QueryRow(ctx,
		`select count(*) from pg_class where relname = 'goose_db_version'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("goose_db_version exists after an invalid retention")
	}
}
