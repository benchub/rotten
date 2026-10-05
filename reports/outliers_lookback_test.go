package reports_test

import (
	"testing"
	"time"

	"github.com/benchub/rotten/internal/testdb"
)

// These tests cover the adaptive lookback: a group with fewer than $8
// samples in the default lookback (the range's length, 1 to 7 days) looks
// further back, window by window, until it has $8, but never more than 7
// days before the range.

func wantHistory(t *testing.T, row map[string]any, samples int64, median float64) {
	t.Helper()
	if got, ok := row["history_samples"].(int64); !ok || got != samples {
		t.Errorf("history_samples = %v, want %d", row["history_samples"], samples)
	}
	if got := asFloat(t, row["history_median_ms"]); got != median {
		t.Errorf("history_median_ms = %v, want %v", got, median)
	}
}

// The backlog's red test: an hourly job has at most 24 samples in the day
// before a 3h range, so it was never scored. It now reaches back to its 30
// most recent windows, and no further: the 18 older slow ones don't count.
func TestOutliersHourlyJobReachesBackForHistory(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	end := fixture.Anchor
	start := end.Add(-3 * time.Hour)
	h := newOutlierHistory(t, conn, fixture, "canvas7p", "hourly_job")
	// 48 hourly runs before the range, oldest first: 18 slow, then 30 normal.
	h.add(start.Add(-48*time.Hour+10*time.Minute), time.Hour, concat(repeat(18, 50), repeat(30, 9, 10, 11))...)
	// One slow run in the range.
	h.add(start.Add(time.Hour), time.Hour, 100)
	fingerprintID := h.finish()

	row := findOutlier(outliersForRange(t, conn, "7", start, end), fingerprintID)
	if row == nil {
		t.Fatalf("hourly fingerprint %d with a slow run in a 3h range was not listed", fingerprintID)
	}
	wantHistory(t, row, 30, 10)
}

// A 24h range's default lookback is a day; it extends the same way.
func TestOutliersDayRangeReachesBackForHistory(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	end := fixture.Anchor
	start := end.Add(-24 * time.Hour)
	h := newOutlierHistory(t, conn, fixture, "canvas7p", "hourly_job_day")
	h.add(start.Add(-40*time.Hour+10*time.Minute), time.Hour, repeat(40, 9, 10, 11)...)
	h.add(start.Add(time.Hour), time.Hour, 100)
	fingerprintID := h.finish()

	row := findOutlier(outliersForRange(t, conn, "7", start, end), fingerprintID)
	if row == nil {
		t.Fatalf("hourly fingerprint %d with a slow run in a 24h range was not listed", fingerprintID)
	}
	wantHistory(t, row, 30, 10)
}

// A frequent query already has 30 samples in the default lookback, so it
// keeps its recent history: all of the last day's samples, and none of the
// slow ones before it, which would otherwise move its median.
func TestOutliersFrequentQueryKeepsDefaultLookback(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	end := fixture.Anchor
	start := end.Add(-3 * time.Hour)
	h := newOutlierHistory(t, conn, fixture, "canvas7p", "frequent")
	// 100 slow samples 2 to 3 days back, then 40 normal ones in the last day.
	h.add(start.Add(-3*24*time.Hour), 10*time.Minute, repeat(100, 25)...)
	h.add(start.Add(-20*time.Hour), 10*time.Minute, repeat(40, 9, 10, 11)...)
	h.add(start.Add(time.Hour), time.Minute, 100)
	fingerprintID := h.finish()

	row := findOutlier(outliersForRange(t, conn, "7", start, end), fingerprintID)
	if row == nil {
		t.Fatalf("frequent fingerprint %d with a slow window was not listed", fingerprintID)
	}
	wantHistory(t, row, 40, 10)
}

// The lookback stops at 7 days: a window starting exactly 7 days before the
// range counts, one starting a minute earlier doesn't. With fewer than 30
// samples in those 7 days, the group isn't scored.
func TestOutliersLookbackStopsAtSevenDays(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	end := fixture.Anchor
	start := end.Add(-3 * time.Hour)
	h := newOutlierHistory(t, conn, fixture, "canvas7p", "rare_job")
	// 29 samples every 5 hours, the oldest 6 days before the range.
	h.add(start.Add(-6*24*time.Hour), 5*time.Hour, repeat(29, 9, 10, 11)...)
	h.add(start.Add(-7*24*time.Hour-time.Minute), time.Minute, 10)
	h.add(start.Add(time.Hour), time.Minute, 100)
	fingerprintID := h.finish()

	if row := findOutlier(outliersForRange(t, conn, "7", start, end), fingerprintID); row != nil {
		t.Fatalf("fingerprint %d with 29 samples in the 7 days before the range was listed: %v", fingerprintID, row)
	}

	h.add(start.Add(-7*24*time.Hour), time.Minute, 10)
	row := findOutlier(outliersForRange(t, conn, "7", start, end), fingerprintID)
	if row == nil {
		t.Fatalf("fingerprint %d with 30 samples in the 7 days before the range was not listed", fingerprintID)
	}
	wantHistory(t, row, 30, 10)
}

// The lookback extends by whole windows: when two hosts share the 30th most
// recent window, both of its samples count.
func TestOutliersLookbackKeepsWholeWindows(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)

	end := fixture.Anchor
	start := end.Add(-3 * time.Hour)
	h := newOutlierHistory(t, conn, fixture, "canvas7p", "tied_window")
	first := start.Add(-40 * time.Hour)
	// The 30th most recent window has two samples; the one before it is slow.
	h.add(first, time.Hour, 50)
	h.add(first.Add(time.Hour), 0, 10, 10)
	h.add(first.Add(2*time.Hour), time.Hour, repeat(29, 9, 10, 11)...)
	h.add(start.Add(time.Hour), time.Minute, 100)
	fingerprintID := h.finish()

	row := findOutlier(outliersForRange(t, conn, "7", start, end), fingerprintID)
	if row == nil {
		t.Fatalf("fingerprint %d was not listed", fingerprintID)
	}
	wantHistory(t, row, 31, 10)
}
