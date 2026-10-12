package reports_test

import (
	"context"
	"math"
	"os"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/testdb"
	"github.com/jackc/pgx/v5"
)

// readFingerprintContextTimeseries runs the per-context series: the time
// series arguments, then the context's controller, action and job tag, each
// nil when the context has none.
func readFingerprintContextTimeseries(t *testing.T, conn *pgx.Conn, args ...any) []fingerprintTimeseriesBucket {
	t.Helper()
	query, err := os.ReadFile("fingerprint_context_timeseries.sql")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(context.Background(), string(query), args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var out []fingerprintTimeseriesBucket
	for rows.Next() {
		var r fingerprintTimeseriesBucket
		if err := rows.Scan(&r.BucketStart, &r.BucketEnd, &r.Calls, &r.TotalMS); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestFingerprintContextTimeseriesCountsOnlyThatContext(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	args := func(controller, action, job any) []any {
		return []any{
			"canvas", testdb.ReportEnvironment, "13",
			fixture.FingerprintID["users"],
			fixture.Anchor.Add(-100 * time.Minute), fixture.Anchor.Add(-20 * time.Minute),
			10 * time.Minute, nil,
			controller, action, job,
		}
	}

	// users#show ran 200 of the newest event's 500 calls, with that share of
	// its 250 ms (the event's six contexts total 491 calls), and all of the
	// other two events' calls.
	got := readFingerprintContextTimeseries(t, conn, args("users", "show", nil)...)
	if len(got) != 8 {
		t.Fatalf("got %d buckets, want 8 (empty ones included): %+v", len(got), got)
	}
	wantCalls := []int64{0, 300, 0, 0, 0, 100, 0, 200}
	wantMS := []float64{0, 150, 0, 0, 0, 50, 0, 250.0 * 200 / 491}
	for i, bucket := range got {
		if bucket.Calls != wantCalls[i] || math.Abs(bucket.TotalMS-wantMS[i]) > 1e-9 {
			t.Errorf("bucket %d = %d calls, %v ms; want %d, %v", i, bucket.Calls, bucket.TotalMS, wantCalls[i], wantMS[i])
		}
	}
	if !got[0].BucketStart.Equal(fixture.Anchor.Add(-100 * time.Minute)) {
		t.Errorf("first bucket starts %v, want aligned to the range start", got[0].BucketStart)
	}

	// Another context of the same fingerprint sees only its own calls.
	got = readFingerprintContextTimeseries(t, conn, args("users", "index", nil)...)
	var total int64
	for _, bucket := range got {
		total += bucket.Calls
	}
	if total != 120 {
		t.Errorf("users#index total calls = %d, want 120", total)
	}

	// A context the fingerprint never ran in gives all-zero buckets, not an error.
	got = readFingerprintContextTimeseries(t, conn, args("nope", "nope", nil)...)
	if len(got) != 8 {
		t.Fatalf("unknown context gave %d buckets, want 8 zero ones", len(got))
	}
	for _, bucket := range got {
		if bucket.Calls != 0 || bucket.TotalMS != 0 {
			t.Errorf("unknown context bucket = %+v, want zero", bucket)
		}
	}
}

func TestFingerprintContextTimeseriesMatchesNullPartsExactly(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	// The users fingerprint ran as the SyncLearners job on bridge 13.
	got := readFingerprintContextTimeseries(t, conn,
		"bridge", testdb.ReportEnvironment, "13",
		fixture.FingerprintID["users"],
		fixture.Anchor.Add(-3*time.Hour), fixture.Anchor,
		time.Hour, nil,
		nil, nil, "SyncLearners",
	)
	var calls int64
	for _, bucket := range got {
		calls += bucket.Calls
	}
	if calls != 25 {
		t.Fatalf("SyncLearners calls = %d, want 25", calls)
	}

	// The untagged context (all three null) must not pick up the job's calls.
	got = readFingerprintContextTimeseries(t, conn,
		"bridge", testdb.ReportEnvironment, "13",
		fixture.FingerprintID["users"],
		fixture.Anchor.Add(-3*time.Hour), fixture.Anchor,
		time.Hour, nil,
		nil, nil, nil,
	)
	for _, bucket := range got {
		if bucket.Calls != 0 {
			t.Fatalf("untagged picked up the job's calls: %+v", got)
		}
	}
}
