package ingest_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/benchub/rotten/internal/ingest"
	"github.com/benchub/rotten/internal/testdb"
)

type homeEvent struct {
	fingerprint, host string
	ago               time.Duration
	calls, time       float64
}

// seedHomeEvents writes fingerprints, hosts and 5-minute events, each
// starting ago before asOf, as the superuser.
func seedHomeEvents(t *testing.T, conn *pgx.Conn, asOf time.Time, events []homeEvent) {
	t.Helper()
	ctx := context.Background()
	var source int64
	if err := conn.QueryRow(ctx, `insert into rotten.logical_sources (project, environment, cluster, role)
		values ('canvas', 'production', '13', 'primary') returning id`).Scan(&source); err != nil {
		t.Fatal(err)
	}
	fps, hosts := map[string]int64{}, map[string]int64{}
	for _, e := range events {
		if _, ok := fps[e.fingerprint]; !ok {
			var id int64
			if err := conn.QueryRow(ctx, `insert into rotten.fingerprints (fingerprint, normalized) values ($1, $1) returning id`,
				e.fingerprint).Scan(&id); err != nil {
				t.Fatal(err)
			}
			fps[e.fingerprint] = id
		}
		if _, ok := hosts[e.host]; !ok {
			var id int64
			if err := conn.QueryRow(ctx, `insert into rotten.physical_sources (fqdn) values ($1) returning id`, e.host).Scan(&id); err != nil {
				t.Fatal(err)
			}
			hosts[e.host] = id
		}
		start := asOf.Add(-e.ago)
		if _, err := conn.Exec(ctx, `insert into rotten.events
			(fingerprint_id, logical_source_id, physical_source_id, observed_window_start, observed_window_end, calls, time)
			values ($1, $2, $3, $4, $5, $6, $7)`,
			fps[e.fingerprint], source, hosts[e.host], start, start.Add(5*time.Minute), e.calls, e.time); err != nil {
			t.Fatal(err)
		}
	}
}

type homeStatsRow struct {
	computedAt, asOf             time.Time
	eventsLastHour, fingerprints int64
	hostsLastHour, hostsLastWeek int64
}

func readHomeStats(t *testing.T, conn *pgx.Conn) homeStatsRow {
	t.Helper()
	var r homeStatsRow
	if err := conn.QueryRow(context.Background(), `select computed_at, as_of, events_last_hour, fingerprints,
		hosts_last_hour, hosts_last_week from rotten.home_stats`).Scan(
		&r.computedAt, &r.asOf, &r.eventsLastHour, &r.fingerprints, &r.hostsLastHour, &r.hostsLastWeek); err != nil {
		t.Fatal(err)
	}
	return r
}

type heatCell struct {
	hour   time.Time
	bucket int
}

func readHeatmap(t *testing.T, conn *pgx.Conn) map[heatCell]int64 {
	t.Helper()
	rows, err := conn.Query(context.Background(), `select hour, duration_bucket, fingerprints from rotten.home_heatmap`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cells := map[heatCell]int64{}
	for rows.Next() {
		var c heatCell
		var n int64
		if err := rows.Scan(&c.hour, &c.bucket, &n); err != nil {
			t.Fatal(err)
		}
		cells[heatCell{c.hour.UTC(), c.bucket}] = n
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return cells
}

func TestRefreshHomeStats(t *testing.T) {
	db := testdb.StartRotten(t)
	su := db.Connect(t)
	ctx := context.Background()
	asOf := time.Date(2026, 10, 12, 12, 30, 0, 0, time.UTC)
	seedHomeEvents(t, su, asOf, []homeEvent{
		// The last hour: 3 events on 2 hosts.
		{"select 1", "db1", 10 * time.Minute, 10, 5},   // 0.5 ms a call: bucket -1
		{"select 2", "db1", 20 * time.Minute, 10, 6},   // 0.6 ms: bucket -1
		{"select 2", "db2", 59 * time.Minute, 2, 4000}, // 2 s: bucket 6
		// Earlier in the week, on a third host.
		{"select 1", "db3", 3 * time.Hour, 1, 0.5},               // same hour cell as below
		{"select 3", "db3", 3*time.Hour + 5*time.Minute, 4, 1.6}, // 0.4 ms: bucket -1
		{"select 3", "db3", 6 * 24 * time.Hour, 1, 0},            // 0 ms clamps to the lowest bucket
		{"select 4", "db3", 6 * 24 * time.Hour, 0, 9},            // no calls: no duration, not in the heat map
		// Older than a week, on a fourth host: counted nowhere but fingerprints.
		{"select 5", "db4", 8 * 24 * time.Hour, 1, 1},
		// In the future of asOf: ignored.
		{"select 1", "db4", -10 * time.Minute, 1, 1},
	})

	ingestConn := db.ConnectAs(t, testdb.IngestRole)
	refreshed, err := ingest.RefreshHomeStats(ctx, ingestConn, asOf)
	if err != nil {
		t.Fatal(err)
	}
	if !refreshed {
		t.Fatal("RefreshHomeStats skipped with no other refresh running")
	}

	ui := db.ConnectAs(t, testdb.UIRole)
	got := readHomeStats(t, ui)
	if !got.asOf.Equal(asOf) || got.computedAt.IsZero() {
		t.Errorf("as_of %v, computed_at %v; want as_of %v and a computed_at", got.asOf, got.computedAt, asOf)
	}
	if got.eventsLastHour != 3 || got.fingerprints != 5 || got.hostsLastHour != 2 || got.hostsLastWeek != 3 {
		t.Errorf("stats %+v; want 3 events in the last hour, 5 fingerprints, 2 hosts in the last hour, 3 in the week", got)
	}

	h := func(ago time.Duration) time.Time { return asOf.Add(-ago).Truncate(time.Hour) }
	want := map[heatCell]int64{
		{h(10 * time.Minute), -1}:   2, // select 1 and select 2
		{h(59 * time.Minute), 6}:    1,
		{h(3 * time.Hour), -1}:      2, // select 1 (0.5 ms) and select 3 (0.4 ms)
		{h(6 * 24 * time.Hour), -4}: 1,
	}
	cells := readHeatmap(t, ui)
	if len(cells) != len(want) {
		t.Errorf("heat map %v; want %v", cells, want)
	}
	for k, v := range want {
		if cells[k] != v {
			t.Errorf("cell %v = %d, want %d (heat map %v)", k, cells[k], v, cells)
		}
	}

	// A second refresh replaces the numbers rather than adding to them.
	later := asOf.Add(2 * time.Hour)
	if _, err := ingest.RefreshHomeStats(ctx, ingestConn, later); err != nil {
		t.Fatal(err)
	}
	got = readHomeStats(t, ui)
	if !got.asOf.Equal(later) || got.eventsLastHour != 0 || got.hostsLastHour != 0 {
		t.Errorf("after a later refresh: %+v; want as_of %v and nothing in the last hour", got, later)
	}
	var stats int
	if err := ui.QueryRow(ctx, "select count(*) from rotten.home_stats").Scan(&stats); err != nil || stats != 1 {
		t.Errorf("home_stats has %d rows (%v), want 1", stats, err)
	}
}

func TestRefreshHomeStatsBuckets(t *testing.T) {
	db := testdb.StartRotten(t)
	su := db.Connect(t)
	asOf := time.Date(2026, 10, 12, 12, 30, 0, 0, time.UTC)
	// Half-decade buckets: bucket b holds [10^(b/2), 10^((b+1)/2)) ms, from -4
	// (under 0.01 ms included) to 10 (100 s and up).
	cases := []struct {
		ms     float64
		bucket int
	}{
		{0.001, -4}, {0.01, -4}, {0.032, -3}, {1, 0}, {3.17, 1}, {10, 2}, {99999, 9}, {100000, 10}, {1e9, 10},
	}
	var events []homeEvent
	for i, c := range cases {
		events = append(events, homeEvent{
			fingerprint: "select " + string(rune('a'+i)), host: "db1",
			ago: time.Duration(i+1) * time.Hour, calls: 1, time: c.ms,
		})
	}
	seedHomeEvents(t, su, asOf, events)
	if _, err := ingest.RefreshHomeStats(context.Background(), db.ConnectAs(t, testdb.IngestRole), asOf); err != nil {
		t.Fatal(err)
	}
	cells := readHeatmap(t, su)
	for i, c := range cases {
		k := heatCell{asOf.Add(-time.Duration(i+1) * time.Hour).Truncate(time.Hour), c.bucket}
		if cells[k] != 1 {
			t.Errorf("%v ms: want 1 fingerprint in bucket %d at %v; heat map %v", c.ms, c.bucket, k.hour, cells)
		}
	}
}

func TestRefreshHomeStatsSkipsWhileAnotherRuns(t *testing.T) {
	db := testdb.StartRotten(t)
	ctx := context.Background()
	holder := db.ConnectAs(t, testdb.IngestRole)
	tx, err := holder.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := ingest.RefreshHomeStats(ctx, tx, time.Now()); err != nil {
		t.Fatal(err)
	}
	refreshed, err := ingest.RefreshHomeStats(ctx, db.ConnectAs(t, testdb.IngestRole), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if refreshed {
		t.Error("a second refresh ran while the first held the lock; want it skipped")
	}
}

func TestHomeStatsPermissions(t *testing.T) {
	db := testdb.StartRotten(t)
	ctx := context.Background()
	if _, err := ingest.RefreshHomeStats(ctx, db.ConnectAs(t, testdb.IngestRole), time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, r := range []string{testdb.UIRole, testdb.ReadonlyRole} {
		c := db.ConnectAs(t, r)
		for _, q := range []string{"select * from rotten.home_stats", "select * from rotten.home_heatmap"} {
			if _, err := c.Exec(ctx, q); err != nil {
				t.Errorf("%s: %s: %v", r, q, err)
			}
		}
		if _, err := c.Exec(ctx, "select rotten.refresh_home_stats(now())"); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("%s: refresh_home_stats: err %v, want permission denied", r, err)
		}
	}
	for _, r := range []string{testdb.IngestRole, testdb.UIRole, testdb.ReadonlyRole} {
		c := db.ConnectAs(t, r)
		for _, q := range []string{"delete from rotten.home_stats", "delete from rotten.home_heatmap",
			"insert into rotten.home_heatmap (hour, duration_bucket, fingerprints) values (now(), 0, 1)"} {
			if _, err := c.Exec(ctx, q); err == nil || !strings.Contains(err.Error(), "permission denied") {
				t.Errorf("%s: %s: err %v, want permission denied", r, q, err)
			}
		}
	}
}

func TestRunHomeStatsRefresherRefreshesAtStart(t *testing.T) {
	db := testdb.StartRotten(t)
	ctx, cancel := context.WithCancel(context.Background())
	var logs bytes.Buffer
	done := make(chan struct{})
	ingestConn := db.ConnectAs(t, testdb.IngestRole)
	go func() {
		defer close(done)
		ingest.RunHomeStatsRefresher(ctx, ingestConn, time.Hour, slog.New(slog.NewTextHandler(&logs, nil)))
	}()
	ui := db.ConnectAs(t, testdb.UIRole)
	deadline := time.Now().Add(30 * time.Second)
	for {
		var n int
		if err := ui.QueryRow(context.Background(), "select count(*) from rotten.home_stats").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("RunHomeStatsRefresher didn't refresh at start")
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	<-done
}
