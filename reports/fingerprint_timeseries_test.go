package reports_test

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/testdb"
	"github.com/jackc/pgx/v5"
)

type fingerprintTimeseriesBucket struct {
	BucketStart time.Time
	BucketEnd   time.Time
	Calls       int64
	TotalMS     float64
}

func readFingerprintTimeseries(t *testing.T, conn *pgx.Conn, args ...any) []fingerprintTimeseriesBucket {
	t.Helper()
	query, err := os.ReadFile("fingerprint_timeseries.sql")
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

func TestFingerprintTimeseriesReturnsBucketsForOneFingerprintAndSource(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	got := readFingerprintTimeseries(t, conn,
		"canvas",
		testdb.ReportEnvironment,
		"13",
		fixture.FingerprintID["users"],
		fixture.Anchor.Add(-100*time.Minute),
		fixture.Anchor.Add(-20*time.Minute),
		10*time.Minute,
		nil,
	)

	want := []fingerprintTimeseriesBucket{
		{fixture.Anchor.Add(-100 * time.Minute), fixture.Anchor.Add(-90 * time.Minute), 0, 0},
		{fixture.Anchor.Add(-90 * time.Minute), fixture.Anchor.Add(-80 * time.Minute), 300, 150},
		{fixture.Anchor.Add(-80 * time.Minute), fixture.Anchor.Add(-70 * time.Minute), 0, 0},
		{fixture.Anchor.Add(-70 * time.Minute), fixture.Anchor.Add(-60 * time.Minute), 0, 0},
		{fixture.Anchor.Add(-60 * time.Minute), fixture.Anchor.Add(-50 * time.Minute), 0, 0},
		{fixture.Anchor.Add(-50 * time.Minute), fixture.Anchor.Add(-40 * time.Minute), 300, 130},
		{fixture.Anchor.Add(-40 * time.Minute), fixture.Anchor.Add(-30 * time.Minute), 0, 0},
		{fixture.Anchor.Add(-30 * time.Minute), fixture.Anchor.Add(-20 * time.Minute), 500, 250},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fingerprint timeseries = %+v, want %+v", got, want)
	}
}

func TestFingerprintTimeseriesKeepsTrailingPartialBucket(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	got := readFingerprintTimeseries(t, conn,
		"canvas",
		testdb.ReportEnvironment,
		"13",
		fixture.FingerprintID["users"],
		fixture.Anchor.Add(-100*time.Minute),
		fixture.Anchor.Add(-20*time.Minute),
		30*time.Minute,
		nil,
	)

	want := []fingerprintTimeseriesBucket{
		{fixture.Anchor.Add(-100 * time.Minute), fixture.Anchor.Add(-70 * time.Minute), 300, 150},
		{fixture.Anchor.Add(-70 * time.Minute), fixture.Anchor.Add(-40 * time.Minute), 300, 130},
		{fixture.Anchor.Add(-40 * time.Minute), fixture.Anchor.Add(-20 * time.Minute), 500, 250},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fingerprint timeseries = %+v, want %+v", got, want)
	}
}

func TestFingerprintTimeseriesUsesFixedDayWidthAcrossDST(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	ctx := context.Background()

	if _, err := conn.Exec(ctx, "set timezone to 'America/Los_Angeles'"); err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, time.March, 7, 8, 0, 0, 0, time.UTC)
	end := time.Date(2026, time.March, 10, 8, 0, 0, 0, time.UTC)
	eventStart := time.Date(2026, time.March, 9, 7, 30, 0, 0, time.UTC)
	insertTimeseriesEvent(t, conn, fixture, "canvas13p", fixture.FingerprintID["users"], eventStart, 11, 22)
	gapEventStart := time.Date(2026, time.March, 10, 7, 30, 0, 0, time.UTC)
	insertTimeseriesEvent(t, conn, fixture, "canvas13p", fixture.FingerprintID["users"], gapEventStart, 13, 26)

	got := readFingerprintTimeseries(t, conn,
		"canvas",
		testdb.ReportEnvironment,
		"13",
		fixture.FingerprintID["users"],
		start,
		end,
		"1 day",
		nil,
	)

	if len(got) != 3 {
		t.Fatalf("got %d buckets, want 3: %+v", len(got), got)
	}
	if !got[2].BucketStart.Equal(start.Add(48*time.Hour)) || !got[2].BucketEnd.Equal(end) {
		t.Fatalf("last bucket = [%s, %s), want [%s, %s)", got[2].BucketStart, got[2].BucketEnd, start.Add(48*time.Hour), end)
	}
	total := int64(0)
	for _, bucket := range got {
		total += bucket.Calls
	}
	if total != 24 {
		t.Fatalf("total calls = %d, want 24; buckets = %+v", total, got)
	}
}

func TestFingerprintTimeseriesRejectsNonPositiveWidths(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	query, err := os.ReadFile("fingerprint_timeseries.sql")
	if err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct {
		name  string
		width any
	}{
		{"zero", time.Duration(0)},
		{"negative_hour", -time.Hour},
		{"negative_normalized_interval", "1 day -25 hours"},
		{"sub_microsecond", 500 * time.Nanosecond},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := runTimeseriesExpectingError(t, conn, string(query),
				"canvas",
				testdb.ReportEnvironment,
				"13",
				fixture.FingerprintID["users"],
				fixture.Anchor.Add(-100*time.Minute),
				fixture.Anchor.Add(-20*time.Minute),
				c.width,
				nil,
			)
			if err == nil {
				t.Fatal("non-positive bucket width succeeded, want an error")
			}
			if !strings.Contains(err.Error(), "bucket width must be positive") {
				t.Fatalf("error = %v, want positive bucket width message", err)
			}
		})
	}
}

func TestFingerprintTimeseriesEmptyForEmptyOrInvertedRanges(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	for _, c := range []struct {
		name  string
		start time.Time
		end   time.Time
	}{
		{"equal", fixture.Anchor, fixture.Anchor},
		{"inverted", fixture.Anchor, fixture.Anchor.Add(-time.Minute)},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := readFingerprintTimeseries(t, conn,
				"canvas",
				testdb.ReportEnvironment,
				"13",
				fixture.FingerprintID["users"],
				c.start,
				c.end,
				10*time.Minute,
				nil,
			)
			if len(got) != 0 {
				t.Fatalf("got %+v, want empty result", got)
			}
		})
	}
}

func TestFingerprintTimeseriesRejectsMonthWidth(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	query, err := os.ReadFile("fingerprint_timeseries.sql")
	if err != nil {
		t.Fatal(err)
	}

	err = runTimeseriesExpectingError(t, conn, string(query),
		"canvas",
		testdb.ReportEnvironment,
		"13",
		fixture.FingerprintID["users"],
		fixture.Anchor.Add(-100*time.Minute),
		fixture.Anchor.Add(-20*time.Minute),
		"1 month",
		nil,
	)
	if err == nil {
		t.Fatal("month bucket width succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "bucket width must not contain months or years") {
		t.Fatalf("error = %v, want month/year bucket width message", err)
	}
}

func TestFingerprintTimeseriesCapsBuckets(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	query, err := os.ReadFile("fingerprint_timeseries.sql")
	if err != nil {
		t.Fatal(err)
	}

	err = runTimeseriesExpectingError(t, conn, string(query),
		"canvas",
		testdb.ReportEnvironment,
		"13",
		fixture.FingerprintID["users"],
		fixture.Anchor.Add(-100*time.Minute),
		fixture.Anchor.Add(-20*time.Minute),
		time.Millisecond,
		nil,
	)
	if err == nil {
		t.Fatal("over-cap bucket request succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "bucket count must be at most 10000") {
		t.Fatalf("error = %v, want bucket cap message", err)
	}
}

func TestFingerprintTimeseriesPrunesEventPartitions(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	start := fixture.Anchor.Add(-100 * time.Minute)
	end := fixture.Anchor.Add(-20 * time.Minute)
	expectedPartitions := partitionsOverlappingRange(t, conn, "rotten.events", start, end)

	plan := explainReport(t, conn, "fingerprint_timeseries.sql",
		"canvas",
		testdb.ReportEnvironment,
		"13",
		fixture.FingerprintID["users"],
		start,
		end,
		10*time.Minute,
		nil,
	)
	assertPlanTouchesOnlyPartitions(t, conn, plan, "rotten.events", expectedPartitions)
}

func TestFingerprintTimeseriesPrunesFixedMidnightCrossingRange(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	start, end := fixedMidnightCrossingRange()
	expectedPartitions := partitionsOverlappingRange(t, conn, "rotten.events", start, end)
	if len(expectedPartitions) != 2 {
		t.Fatalf("fixed range touches %d partitions, want 2", len(expectedPartitions))
	}

	plan := explainReport(t, conn, "fingerprint_timeseries.sql",
		"canvas",
		testdb.ReportEnvironment,
		"13",
		fixture.FingerprintID["users"],
		start,
		end,
		10*time.Minute,
		nil,
	)
	assertPlanTouchesOnlyPartitions(t, conn, plan, "rotten.events", expectedPartitions)
}

func insertTimeseriesEvent(t *testing.T, conn *pgx.Conn, fixture *testdb.Reports, sourceKey string, fingerprintID int64, start time.Time, calls float64, totalMS float64) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `insert into rotten.events
		(fingerprint_id, logical_source_id, physical_source_id, observed_window_start, observed_window_end, calls, time)
		values ($1,$2,$3,$4,$5,$6,$7)`,
		fingerprintID, fixture.SourceIDs[sourceKey], fixture.PhysicalIDs[sourceKey], start, start.Add(testdb.WindowLength), calls, totalMS); err != nil {
		t.Fatal(err)
	}
}

func runTimeseriesExpectingError(t *testing.T, conn *pgx.Conn, query string, args ...any) error {
	t.Helper()
	rows, err := conn.Query(context.Background(), query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
	}
	return rows.Err()
}
