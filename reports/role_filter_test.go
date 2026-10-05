package reports_test

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/testdb"
)

// The top-query, outlier and timeseries reports take an optional role as
// their last parameter. NULL means every role in the project, environment
// and cluster, as before; a role narrows the source filter to it.

type roleTotals struct {
	FingerprintID int64
	Calls         float64
	TotalMS       float64
}

func TestTopReportsFilterByRole(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	ctx := context.Background()

	recentTotals := func(sourceKey, fingerprint string) roleTotals {
		var out roleTotals
		out.FingerprintID = fixture.FingerprintID[fingerprint]
		for _, e := range testdb.ReportEvents {
			if e.Recent() && e.Source == sourceKey && e.Fingerprint == fingerprint {
				out.Calls += e.Calls
				out.TotalMS += e.Time
			}
		}
		return out
	}

	cases := []struct {
		report string
		role   any
		want   []roleTotals
	}{
		{"top_by_calls.sql", testdb.ReportReplicaRole, []roleTotals{
			recentTotals("canvas13r", "users"), recentTotals("canvas13r", "jobs"),
		}},
		{"top_by_total_time.sql", testdb.ReportReplicaRole, []roleTotals{
			recentTotals("canvas13r", "jobs"), recentTotals("canvas13r", "users"),
		}},
		{"top_by_calls.sql", testdb.ReportPrimaryRole, []roleTotals{
			recentTotals("canvas13p", "users"), recentTotals("canvas13p", "courses"), recentTotals("canvas13p", "jobs"),
		}},
		{"top_by_calls.sql", "no-such-role", nil},
	}
	for _, c := range cases {
		query, err := os.ReadFile(c.report)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := conn.Query(ctx, string(query),
			"canvas", testdb.ReportEnvironment, "13",
			fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor, 10, c.role, nil)
		if err != nil {
			t.Fatalf("%s role %v: %v", c.report, c.role, err)
		}
		var got []roleTotals
		for rows.Next() {
			var (
				r       roleTotals
				avg     float64
				example string
				ctxJSON []byte
			)
			if err := rows.Scan(&r.FingerprintID, &r.Calls, &r.TotalMS, &avg, &example, &ctxJSON, new(bool)); err != nil {
				t.Fatal(err)
			}
			got = append(got, r)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s role %v = %+v, want %+v", c.report, c.role, got, c.want)
		}
	}
}

func TestOutliersFilterByRole(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	for _, c := range []struct {
		role any
		want int
	}{
		{nil, 1},
		{testdb.ReportPrimaryRole, 1},
		{testdb.ReportReplicaRole, 0},
	} {
		got := readOutliers(t, conn,
			"canvas", testdb.ReportEnvironment, "7",
			fixture.Anchor.Add(-testdb.RecentRange), fixture.Anchor,
			10, defaultSigma, defaultMinHistory, defaultRatio, c.role, nil)
		if len(got) != c.want {
			t.Fatalf("role %v: got %d outliers, want %d: %+v", c.role, len(got), c.want, got)
		}
		for _, row := range got {
			if row.FingerprintID != fixture.FingerprintID["slow"] || row.Role != testdb.ReportPrimaryRole {
				t.Errorf("role %v: unexpected outlier %+v", c.role, row)
			}
		}
	}
}

func TestFingerprintTimeseriesFiltersByRole(t *testing.T) {
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
		testdb.ReportReplicaRole,
	)
	want := []fingerprintTimeseriesBucket{
		{fixture.Anchor.Add(-100 * time.Minute), fixture.Anchor.Add(-70 * time.Minute), 0, 0},
		{fixture.Anchor.Add(-70 * time.Minute), fixture.Anchor.Add(-40 * time.Minute), 200, 80},
		{fixture.Anchor.Add(-40 * time.Minute), fixture.Anchor.Add(-20 * time.Minute), 0, 0},
	}
	if len(got) != len(want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for i := range want {
		if !got[i].BucketStart.Equal(want[i].BucketStart) || !got[i].BucketEnd.Equal(want[i].BucketEnd) ||
			got[i].Calls != want[i].Calls || got[i].TotalMS != want[i].TotalMS {
			t.Errorf("bucket %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}
