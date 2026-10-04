package ingest_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/benchub/rotten/internal/ingest"
	"github.com/benchub/rotten/internal/testdb"
)

// insertPreMigrationContexts inserts events and their contexts the way a
// server from before migration 0011 does, leaving logical_source_id and
// attributed_time null. Each event gets two contexts, with c 1 and 3.
func insertPreMigrationContexts(t *testing.T, conn *pgx.Conn, fixture *testdb.Reports, events int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < events; i++ {
		start := fixture.Anchor.Add(-time.Duration(i+1) * testdb.WindowLength)
		var eventID int64
		if err := conn.QueryRow(ctx, `insert into rotten.events
			(fingerprint_id, logical_source_id, physical_source_id, observed_window_start, observed_window_end, calls, time)
			values ($1, $2, $3, $4, $5, 4, $6) returning id`,
			fixture.FingerprintID["users"], fixture.SourceIDs["canvas7r"], fixture.PhysicalIDs["canvas7r"],
			start, start.Add(testdb.WindowLength), 10*float64(i+1)).Scan(&eventID); err != nil {
			t.Fatal(err)
		}
		for _, c := range []int{1, 3} {
			if _, err := conn.Exec(ctx, `insert into rotten.event_context
				(event_id, observed_window_start, observed_window_end, c)
				values ($1, $2, $3, $4)`, eventID, start, start.Add(testdb.WindowLength), c); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func contextsMissingUtilization(t *testing.T, conn *pgx.Conn) int {
	t.Helper()
	var n int
	if err := conn.QueryRow(context.Background(), `select count(*) from rotten.event_context
		where logical_source_id is null or attributed_time is null`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRepairContextUtilizationFillsPreMigrationRowsInBatches(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	ctx := context.Background()
	insertPreMigrationContexts(t, conn, fixture, 5)

	ingestConn := db.ConnectAs(t, testdb.IngestRole)
	repaired, err := ingest.RepairContextUtilization(ctx, ingestConn, 2)
	if err != nil {
		t.Fatal(err)
	}
	if repaired != 10 || contextsMissingUtilization(t, conn) != 0 {
		t.Fatalf("repaired %d rows, %d still missing; want 10 and 0", repaired, contextsMissingUtilization(t, conn))
	}
	var matched int
	if err := conn.QueryRow(ctx, `
		select count(*) from rotten.event_context ec
		join rotten.events e on e.id = ec.event_id and e.observed_window_start = ec.observed_window_start
		where e.logical_source_id = $1
		  and ec.logical_source_id = e.logical_source_id
		  and ec.attributed_time = e.time * ec.c::double precision / 4::double precision
		  and e.calls = 4`, fixture.SourceIDs["canvas7r"]).Scan(&matched); err != nil {
		t.Fatal(err)
	}
	if matched != 10 {
		t.Errorf("%d repaired rows match time * c / sum(c), want 10", matched)
	}

	again, err := ingest.RepairContextUtilization(ctx, ingestConn, 2)
	if err != nil || again != 0 {
		t.Errorf("repair with nothing left = %d, %v; want 0, nil", again, err)
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// serve runs the repair as soon as it starts, not only after the first
// interval, so rows a pre-0011 server wrote get fixed right after a restart.
func TestRunContextRepairRepairsAtStartupAndLogsCount(t *testing.T) {
	db := testdb.StartRotten(t)
	fixture := testdb.SeedReports(t, db)
	conn := db.Connect(t)
	insertPreMigrationContexts(t, conn, fixture, 3)

	var logs syncBuffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	ingestConn := db.ConnectAs(t, testdb.IngestRole)
	repairCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ingest.RunContextRepair(repairCtx, ingestConn, time.Hour, logger)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("context repair did not stop")
		}
	})

	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(logs.String(), "repaired context utilization") {
		if time.Now().After(deadline) {
			t.Fatalf("no repair log after 10s; logs %q", logs.String())
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !strings.Contains(logs.String(), "rows=6") {
		t.Errorf("repair log %q, want rows=6", logs.String())
	}
	cancel()
	<-done
	if n := contextsMissingUtilization(t, conn); n != 0 {
		t.Errorf("%d contexts still missing utilization after startup repair, want 0", n)
	}
}
