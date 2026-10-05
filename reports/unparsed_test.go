package reports_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/testdb"
	"github.com/jackc/pgx/v5"
)

// readReportMaps runs a report and returns its rows keyed by column name.
func readReportMaps(t *testing.T, conn *pgx.Conn, file string, args ...any) []map[string]any {
	t.Helper()
	query, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(context.Background(), string(query), args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Fatal(err)
		}
		row := map[string]any{}
		for i, f := range rows.FieldDescriptions() {
			row[f.Name] = vals[i]
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func markUnparsed(t *testing.T, conn *pgx.Conn, id int64) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), "update rotten.fingerprints set unparsed = true where id = $1", id); err != nil {
		t.Fatal(err)
	}
}

// The top and outlier reports say which rows are fallback fingerprints, so
// the UI can badge them.
func TestReportsFlagUnparsedFingerprints(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	markUnparsed(t, conn, fixture.FingerprintID["users"])
	markUnparsed(t, conn, fixture.FingerprintID["slow"])
	start, end := fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor

	for _, file := range []string{"top_by_calls.sql", "top_by_total_time.sql"} {
		rows := readReportMaps(t, conn, file, "canvas", testdb.ReportEnvironment, "13", start, end, 10, nil, nil)
		if len(rows) < 2 {
			t.Fatalf("%s: want several rows, got %v", file, rows)
		}
		for _, row := range rows {
			want := row["fingerprint_id"] == fixture.FingerprintID["users"]
			if got, ok := row["unparsed"].(bool); !ok || got != want {
				t.Errorf("%s: fingerprint %v unparsed = %v, want %v", file, row["fingerprint_id"], row["unparsed"], want)
			}
		}
	}
	rows := readReportMaps(t, conn, "outliers.sql", "canvas", testdb.ReportEnvironment, "7", start, end, 10,
		defaultSigma, defaultMinHistory, defaultRatio, nil, nil)
	if len(rows) != 1 || rows[0]["fingerprint_id"] != fixture.FingerprintID["slow"] || rows[0]["unparsed"] != true {
		t.Errorf("outliers: want the slow fingerprint flagged unparsed, got %v", rows)
	}
}

// unparsed_summary counts the fallback fingerprints and their calls in the
// range, with the same source filter as the top reports.
func TestUnparsedSummary(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	start, end := fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor
	summary := func(role any) (int64, float64) {
		t.Helper()
		rows := readReportMaps(t, conn, "unparsed_summary.sql", "canvas", testdb.ReportEnvironment, "13", start, end, role)
		if len(rows) != 1 {
			t.Fatalf("want one summary row, got %v", rows)
		}
		fingerprints, _ := rows[0]["fingerprints"].(int64)
		calls, _ := rows[0]["calls"].(float64)
		return fingerprints, calls
	}

	if n, calls := summary(nil); n != 0 || calls != 0 {
		t.Fatalf("nothing unparsed: got %d fingerprints, %v calls", n, calls)
	}

	markUnparsed(t, conn, fixture.FingerprintID["users"])
	markUnparsed(t, conn, fixture.FingerprintID["jobs"])
	want := map[string]float64{}
	for _, e := range testdb.ReportEvents {
		if e.Recent() && (e.Fingerprint == "users" || e.Fingerprint == "jobs") {
			want[e.Source] += e.Calls
		}
	}
	if want["canvas13p"] == 0 || want["canvas13r"] == 0 {
		t.Fatalf("fixture changed: want recent users or jobs calls on both canvas13 roles, got %v", want)
	}
	if n, calls := summary(nil); n != 2 || calls != want["canvas13p"]+want["canvas13r"] {
		t.Errorf("every role: got %d fingerprints, %v calls; want 2, %v", n, calls, want["canvas13p"]+want["canvas13r"])
	}
	if _, calls := summary(testdb.ReportReplicaRole); calls != want["canvas13r"] {
		t.Errorf("replica: got %v calls, want %v", calls, want["canvas13r"])
	}

	// A window that straddles the range's end isn't counted, as in the top reports.
	insertOutlierEvent(t, conn, fixture.SourceIDs["canvas13p"], fixture.PhysicalIDs["canvas13p"], fixture.FingerprintID["users"],
		end.Add(-testdb.WindowLength/2), 1000, 1)
	insertOutlierEvent(t, conn, fixture.SourceIDs["canvas13p"], fixture.PhysicalIDs["canvas13p"], fixture.FingerprintID["users"],
		start.Add(-time.Hour), 1000, 1)
	if _, calls := summary(nil); calls != want["canvas13p"]+want["canvas13r"] {
		t.Errorf("out-of-range windows counted: got %v calls, want %v", calls, want["canvas13p"]+want["canvas13r"])
	}
}
