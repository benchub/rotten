//go:build perf

package reports_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/ingest"
	"github.com/benchub/rotten/internal/testdb"
)

// Budgets for the landing page's stats (migration 0016). The refresh runs in
// rotten-server every 5 minutes, off any request, so it gets a minute: well
// under the interval. The UI's read of the refreshed tables is per request,
// so it must stay as fast as any page.
const (
	homeStatsRefreshBudget = time.Minute
	homeStatsReadBudget    = 50 * time.Millisecond
)

// TestPerfHomeStats times refresh_home_stats over the main seed's last week
// (about a third of its events) as rotten_ingest, the way the server calls
// it, and the UI's read of the result as rotten_ui.
func TestPerfHomeStats(t *testing.T) {
	db := testdb.StartRotten(t)
	f := seedPerf(t, db, false)
	ctx := context.Background()
	ingestConn := db.ConnectAs(t, testdb.IngestRole)
	ui := db.ConnectAs(t, testdb.UIRole)

	median := func(runs []time.Duration) time.Duration {
		sort.Slice(runs, func(i, j int) bool { return runs[i] < runs[j] })
		return runs[len(runs)/2]
	}

	var refreshes []time.Duration
	for i := 0; i < 4; i++ {
		start := time.Now()
		ok, err := ingest.RefreshHomeStats(ctx, ingestConn, f.anchor)
		if err != nil || !ok {
			t.Fatalf("refresh_home_stats: refreshed %v, %v", ok, err)
		}
		if i > 0 { // the first run is the warmup
			refreshes = append(refreshes, time.Since(start))
		}
	}

	var events, fingerprints, hostsWeek int64
	var cells int
	if err := ui.QueryRow(ctx, `select events_last_hour, fingerprints, hosts_last_week,
		(select count(*) from rotten.home_heatmap) from rotten.home_stats`).Scan(&events, &fingerprints, &hostsWeek, &cells); err != nil {
		t.Fatal(err)
	}
	if events == 0 || fingerprints == 0 || hostsWeek == 0 || cells == 0 {
		t.Fatalf("home stats look empty: %d events last hour, %d fingerprints, %d hosts, %d cells", events, fingerprints, hostsWeek, cells)
	}

	var reads []time.Duration
	for i := 0; i < 6; i++ {
		start := time.Now()
		if _, err := ui.Exec(ctx, "select * from rotten.home_stats"); err != nil {
			t.Fatal(err)
		}
		rows, err := ui.Query(ctx, "select hour, duration_bucket, fingerprints from rotten.home_heatmap")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if i > 0 {
			reads = append(reads, time.Since(start))
		}
	}

	refresh, read := median(refreshes), median(reads)
	t.Logf("home stats on %d events: refresh median %v (%v), UI read median %v; %d events last hour, %d fingerprints, %d hosts last week, %d heat map cells",
		f.events, refresh.Round(time.Millisecond), refreshes, read.Round(time.Microsecond), events, fingerprints, hostsWeek, cells)
	if refresh > homeStatsRefreshBudget {
		t.Errorf("refresh median %v, over the %v budget", refresh, homeStatsRefreshBudget)
	}
	if read > homeStatsReadBudget {
		t.Errorf("UI read median %v, over the %v budget", read, homeStatsReadBudget)
	}
}
