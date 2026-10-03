package reports_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/testdb"
	"github.com/jackc/pgx/v5"
)

type fingerprintSourceRow struct {
	Role               string
	Calls              float64
	TotalMS            float64
	AvgMSPerCall       *float64
	HistorySamples     *int64
	HistoryMeanMS      *float64
	HistoryDeviationMS *float64
}

func readFingerprintSources(t *testing.T, conn *pgx.Conn, args ...any) []fingerprintSourceRow {
	t.Helper()
	query, err := os.ReadFile("fingerprint_sources.sql")
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
		if err := rows.Scan(&r.Role, &r.Calls, &r.TotalMS, &r.AvgMSPerCall, &r.HistorySamples, &r.HistoryMeanMS, &r.HistoryDeviationMS); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func float64Ptr(v float64) *float64 { return &v }
func int64Ptr(v int64) *int64       { return &v }

func TestFingerprintSourcesOneRowPerRole(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	got := readFingerprintSources(t, conn,
		"canvas", testdb.ReportEnvironment, "13",
		fixture.FingerprintID["users"],
		fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor,
		nil,
	)

	want := []fingerprintSourceRow{
		{testdb.ReportPrimaryRole, 900, 450, float64Ptr(0.5), int64Ptr(30000), float64Ptr(0.5), float64Ptr(0.1)},
		{testdb.ReportReplicaRole, 200, 80, float64Ptr(0.4), nil, nil, nil},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sources = %s, want %s", formatSourceRows(got), formatSourceRows(want))
	}
}

func TestFingerprintSourcesFilterByRole(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	got := readFingerprintSources(t, conn,
		"canvas", testdb.ReportEnvironment, "13",
		fixture.FingerprintID["users"],
		fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor,
		testdb.ReportReplicaRole,
	)
	want := []fingerprintSourceRow{{testdb.ReportReplicaRole, 200, 80, float64Ptr(0.4), nil, nil, nil}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sources = %s, want %s", formatSourceRows(got), formatSourceRows(want))
	}
}

func TestFingerprintSourcesKeepHistoryWithoutEventsInRange(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	// No users windows on canvas 13 fall in the last 20 minutes. The primary
	// still has history; the replica has neither and is left out.
	got := readFingerprintSources(t, conn,
		"canvas", testdb.ReportEnvironment, "13",
		fixture.FingerprintID["users"],
		fixture.Anchor.Add(-20*time.Minute), fixture.Anchor,
		nil,
	)
	want := []fingerprintSourceRow{{testdb.ReportPrimaryRole, 0, 0, nil, int64Ptr(30000), float64Ptr(0.5), float64Ptr(0.1)}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sources = %s, want %s", formatSourceRows(got), formatSourceRows(want))
	}
}

func TestFingerprintSourcesScopedToProjectAndFingerprint(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	got := readFingerprintSources(t, conn,
		"canvas", testdb.ReportEnvironment, "7",
		fixture.FingerprintID["slow"],
		fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor,
		nil,
	)
	want := []fingerprintSourceRow{
		{testdb.ReportPrimaryRole, 20, 800, float64Ptr(40), int64Ptr(801), float64Ptr(8.039950062421973), float64Ptr(1.4447800862079763)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sources = %s, want %s", formatSourceRows(got), formatSourceRows(want))
	}
}

func TestFingerprintSourcesPrunePartitions(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	start, end := fixedMidnightCrossingRange()
	expected := partitionsOverlappingRange(t, conn, "rotten.events", start, end)
	if len(expected) != 2 {
		t.Fatalf("fixed range touches %d partitions, want 2", len(expected))
	}
	plan := explainReport(t, conn, "fingerprint_sources.sql",
		"canvas", testdb.ReportEnvironment, "13",
		fixture.FingerprintID["users"],
		start, end,
		nil,
	)
	assertPlanTouchesOnlyPartitions(t, conn, plan, "rotten.events", expected)
}

func formatSourceRows(rows []fingerprintSourceRow) string {
	parts := make([]string, 0, len(rows))
	for _, r := range rows {
		parts = append(parts, fmt.Sprintf("%s calls=%v total=%v avg=%s samples=%s mean=%s dev=%s",
			r.Role, r.Calls, r.TotalMS, ptrString(r.AvgMSPerCall), ptrString(r.HistorySamples),
			ptrString(r.HistoryMeanMS), ptrString(r.HistoryDeviationMS)))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func ptrString[T any](p *T) string {
	if p == nil {
		return "nil"
	}
	return fmt.Sprint(*p)
}
