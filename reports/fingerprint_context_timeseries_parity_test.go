package reports_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/testdb"
)

// The context series has the same bucket logic as fingerprint_timeseries.sql
// by copy, not by sharing. They're separate files run with bound parameters,
// so sharing the CTEs would take a view or function in the schema for two
// small blocks. This test keeps the copies in step: the same buckets for the
// same arguments, and the same errors.
func TestFingerprintContextTimeseriesBucketsMatchTimeseries(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	plain, err := os.ReadFile("fingerprint_timeseries.sql")
	if err != nil {
		t.Fatal(err)
	}
	scoped, err := os.ReadFile("fingerprint_context_timeseries.sql")
	if err != nil {
		t.Fatal(err)
	}
	run := func(query []byte, start, end time.Time, width any, scopedToContext bool) ([]fingerprintTimeseriesBucket, error) {
		args := []any{[]string{"canvas"}, []string{testdb.ReportEnvironment}, []string{"13"}, fixture.FingerprintID["users"], start, end, width, nil}
		if scopedToContext {
			args = append(args, []string{"users#show#"}, false)
		}
		rows, err := conn.Query(context.Background(), string(query), args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []fingerprintTimeseriesBucket
		for rows.Next() {
			var r fingerprintTimeseriesBucket
			if err := rows.Scan(&r.BucketStart, &r.BucketEnd, &r.Calls, &r.TotalMS); err != nil {
				return nil, err
			}
			out = append(out, r)
		}
		return out, rows.Err()
	}

	start := fixture.Anchor.Add(-100 * time.Minute)
	end := fixture.Anchor.Add(-40 * time.Minute)
	for _, c := range []struct {
		name  string
		start time.Time
		end   time.Time
		width any
	}{
		{"ten_minutes", start, end, 10 * time.Minute},
		// 7 minutes doesn't divide the range, and the 10-minute event
		// windows straddle bucket edges, so each goes to the bucket holding
		// its start.
		{"straddling_edges", start, end, 7 * time.Minute},
		{"short_last_bucket", start, end, 25 * time.Minute},
		{"one_day", start, end, "1 day"},
		{"empty_range", start, start, 10 * time.Minute},
		{"inverted_range", start, start.Add(-time.Minute), 10 * time.Minute},
		{"month", start, fixture.Anchor, "1 month"},
		{"year", start, fixture.Anchor, "1 year"},
		{"zero", start, fixture.Anchor, time.Duration(0)},
		{"negative", start, fixture.Anchor, -time.Hour},
		{"negative_normalized", start, fixture.Anchor, "1 day -25 hours"},
		{"sub_microsecond", start, fixture.Anchor, 500 * time.Nanosecond},
		{"too_many_buckets", start, start.Add(3 * time.Hour), time.Second},
		{"exactly_max_buckets", start, start.Add(10000 * time.Second), time.Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			wantRows, wantErr := run(plain, c.start, c.end, c.width, false)
			gotRows, gotErr := run(scoped, c.start, c.end, c.width, true)
			if (wantErr == nil) != (gotErr == nil) {
				t.Fatalf("errors differ: timeseries %v, context series %v", wantErr, gotErr)
			}
			if wantErr != nil {
				want := strings.ReplaceAll(wantErr.Error(), "fingerprint_timeseries", "fingerprint_context_timeseries")
				if gotErr.Error() != want {
					t.Fatalf("context series error = %v, want %v", gotErr, want)
				}
				return
			}
			if len(gotRows) != len(wantRows) {
				t.Fatalf("got %d buckets, want %d", len(gotRows), len(wantRows))
			}
			for i := range wantRows {
				if !gotRows[i].BucketStart.Equal(wantRows[i].BucketStart) || !gotRows[i].BucketEnd.Equal(wantRows[i].BucketEnd) {
					t.Fatalf("bucket %d = [%s, %s), want [%s, %s)", i, gotRows[i].BucketStart, gotRows[i].BucketEnd,
						wantRows[i].BucketStart, wantRows[i].BucketEnd)
				}
			}
		})
	}
}
