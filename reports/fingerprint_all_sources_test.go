package reports_test

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/testdb"
	"github.com/jackc/pgx/v5"
)

// readFingerprintAllSources returns the all-sources row as a
// fingerprintSourceRow with Role "".
func readFingerprintAllSources(t *testing.T, conn *pgx.Conn, args ...any) []fingerprintSourceRow {
	t.Helper()
	query, err := os.ReadFile("fingerprint_all_sources.sql")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(context.Background(), string(query), args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var out []fingerprintSourceRow
	for rows.Next() {
		var r fingerprintSourceRow
		if err := rows.Scan(&r.Calls, &r.TotalMS, &r.AvgMSPerCall, &r.HistorySamples, &r.HistoryMeanMS, &r.HistoryDeviationMS); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestFingerprintAllSourcesSumsEverySourceInRange(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.ConnectAs(t, testdb.UIRole)

	// users runs recently on every source: canvas 13 primary 900 calls and
	// 450 ms, replica 200 and 80, canvas 7 primary 60 and 30, replica 140
	// and 70, bridge 13 primary 25 and 10. Its old windows are left out.
	got := readFingerprintAllSources(t, conn,
		fixture.FingerprintID["users"],
		fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor,
	)
	want := []fingerprintSourceRow{
		{"", 1325, 640, float64Ptr(640.0 / 1325), int64Ptr(50000), float64Ptr(0.5), float64Ptr(0.2)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("all sources = %s, want %s", formatSourceRows(got), formatSourceRows(want))
	}
}

func TestFingerprintAllSourcesHistoryIsSourceZeroMeanTime(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	// slow has source-0 rows for mean_time and calls, and a canvas7p
	// mean_time row; only source 0's mean_time is the all-sources history.
	got := readFingerprintAllSources(t, conn,
		fixture.FingerprintID["slow"],
		fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor,
	)
	want := []fingerprintSourceRow{
		{"", 20, 800, float64Ptr(40), int64Ptr(1001), float64Ptr(5.034965034965035), float64Ptr(1.4908977911903367)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("all sources = %s, want %s", formatSourceRows(got), formatSourceRows(want))
	}
}

func TestFingerprintAllSourcesWithoutHistory(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	// courses: canvas 13 primary 100 calls and 300 ms, bridge 13 primary 75
	// and 150, recently; no fingerprint_stats.
	got := readFingerprintAllSources(t, conn,
		fixture.FingerprintID["courses"],
		fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor,
	)
	want := []fingerprintSourceRow{{"", 175, 450, float64Ptr(450.0 / 175), nil, nil, nil}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("all sources = %s, want %s", formatSourceRows(got), formatSourceRows(want))
	}
}

func TestFingerprintAllSourcesKeepHistoryWithoutEventsInRange(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	got := readFingerprintAllSources(t, conn,
		fixture.FingerprintID["users"],
		fixture.Anchor.Add(-20*time.Minute), fixture.Anchor,
	)
	want := []fingerprintSourceRow{{"", 0, 0, nil, int64Ptr(50000), float64Ptr(0.5), float64Ptr(0.2)}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("all sources = %s, want %s", formatSourceRows(got), formatSourceRows(want))
	}
}

func TestFingerprintAllSourcesEmptyWithoutEventsOrHistory(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	got := readFingerprintAllSources(t, conn,
		fixture.FingerprintID["courses"],
		fixture.Anchor.Add(-15*time.Minute), fixture.Anchor,
	)
	if len(got) != 0 {
		t.Fatalf("all sources = %s, want no rows", formatSourceRows(got))
	}
}

func TestFingerprintAllSourcesExcludesWindowsStraddlingTheEnd(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	// The canvas 13 primary window 30 minutes back (500 calls, 250 ms) starts
	// inside the range but ends 5 minutes after it, so it's left out.
	got := readFingerprintAllSources(t, conn,
		fixture.FingerprintID["users"],
		fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor.Add(-25*time.Minute),
	)
	want := []fingerprintSourceRow{
		{"", 825, 390, float64Ptr(390.0 / 825), int64Ptr(50000), float64Ptr(0.5), float64Ptr(0.2)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("all sources = %s, want %s", formatSourceRows(got), formatSourceRows(want))
	}
}

func TestFingerprintAllSourcesPrunePartitions(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	start, end := fixedMidnightCrossingRange()
	expected := partitionsOverlappingRange(t, conn, "rotten.events", start, end)
	if len(expected) != 2 {
		t.Fatalf("fixed range touches %d partitions, want 2", len(expected))
	}
	plan := explainReport(t, conn, "fingerprint_all_sources.sql",
		fixture.FingerprintID["users"],
		start, end,
	)
	assertPlanTouchesOnlyPartitions(t, conn, plan, "rotten.events", expected)
}
