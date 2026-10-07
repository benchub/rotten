package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	"github.com/benchub/rotten/internal/auth"
	fingerprinting "github.com/benchub/rotten/internal/fingerprint"
	"github.com/benchub/rotten/internal/ingest"
	"github.com/benchub/rotten/internal/pgss"
	"github.com/benchub/rotten/internal/pssc"
	"github.com/benchub/rotten/internal/testdb"
)

// A statement the pinned Postgres 17 parser rejects, read from a real
// Postgres 18 pg_stat_statements, still reaches the server: its calls, time
// and contexts land under a text-derived fallback fingerprint marked unparsed.
func TestUnparseableStatementReachesServerAsUnparsedFallback(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped under -short")
	}
	ctx := context.Background()
	observed := testdb.StartObserved(t, 18)
	conn := observed.Connect(t)
	if _, err := conn.Exec(ctx, "create table unparsed_widgets (id int, name text)"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "insert into unparsed_widgets values (1, 'a')"); err != nil {
		t.Fatal(err)
	}
	// Postgres 18 drops leading comments, so the context is appended.
	const update = "update unparsed_widgets set name = 'b' where id = 1 returning with (old as o, new as n) o.name, n.name /*controller:users,action:update*/"
	for range 3 {
		if _, err := conn.Exec(ctx, update); err != nil {
			t.Fatal(err)
		}
	}

	reader := pgss.NewReader(conn)
	stats, err := reader.ReadStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	texts := pgss.NewTextCache(reader)
	if err := texts.Fill(ctx, stats); err != nil {
		t.Fatal(err)
	}
	var deltas []pgss.Delta
	var pgssText string
	for _, s := range stats {
		if strings.HasPrefix(s.Query, "update unparsed_widgets") {
			deltas = append(deltas, pgss.Delta{Stat: s})
			pgssText = s.Query
		}
	}
	if len(deltas) != 1 || deltas[0].Calls != 3 {
		t.Fatalf("want one update entry with 3 calls, got %d entries: %+v", len(deltas), deltas)
	}
	if _, err := fingerprinting.Normalized(pgssText, fingerprinting.Options{}); err == nil {
		t.Fatalf("the pinned parser accepts %q; pick other syntax", pgssText)
	}

	rotten := testdb.StartRotten(t)
	owner, err := pgxpool.New(ctx, rotten.DSNAs(t, testdb.OwnerRole))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(owner.Close)
	ingestPool, err := pgxpool.New(ctx, rotten.DSNAs(t, testdb.IngestRole))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ingestPool.Close)
	created, err := auth.CreateKey(ctx, owner, "unparsed-worker", "db-unparsed.example", "test")
	if err != nil {
		t.Fatal(err)
	}
	keyID, _, err := auth.ParseKey(created.Token)
	if err != nil {
		t.Fatal(err)
	}
	authed := auth.NewContext(ctx, auth.Key{ID: keyID, Name: "unparsed-worker", FQDN: "db-unparsed.example"})
	handler := ingest.NewHandler(ingestPool)
	reg, err := handler.Register(authed, connect.NewRequest(&rottenv1.RegisterRequest{
		Project: "unparsed", Environment: "test", Cluster: "cluster", Role: "primary", Fqdn: "db-unparsed.example",
	}))
	if err != nil {
		t.Fatal(err)
	}

	c, a, j := sampleRegexes(t)
	w := New(Config{
		LogicalID:    reg.Msg.GetLogicalSourceId(),
		PhysicalID:   reg.Msg.GetPhysicalSourceId(),
		ReController: c,
		ReAction:     a,
		ReJobTag:     j,
	}, RealClock{})
	end := time.Now().UTC().Truncate(time.Second)
	// Contexts come from pssc: every entry counts in full, as on a first
	// window after the statements started.
	psscStats, ok, err := pssc.NewReader(conn).ReadStats(ctx)
	if err != nil || !ok {
		t.Fatalf("pssc ReadStats ok=%v err=%v", ok, err)
	}
	psscDeltas, _ := pssc.Diff(pssc.Snapshot{}, psscStats)
	contexts := newPSSCContexts(true, psscDeltas, pssc.Snapshot{}, pssc.Snapshot{}, time.Time{})
	batch, _, _ := w.buildHarvestBatchAndSnapshotPSSC(ctx, texts, pgss.Snapshot{}, pgss.Snapshot{}, deltas, contexts, end.Add(-time.Minute), end)
	if len(batch.GetAggregates()) != 1 {
		t.Fatalf("want the unparseable entry in the batch, got %d aggregates", len(batch.GetAggregates()))
	}
	agg := batch.GetAggregates()[0]
	if !agg.GetUnparsed() {
		t.Fatalf("aggregate isn't marked unparsed: %v", agg)
	}
	if _, err := handler.SubmitHarvest(authed, connect.NewRequest(batch)); err != nil {
		t.Fatalf("SubmitHarvest: %v", err)
	}

	wantFP := fingerprinting.Fallback(pgssText).Fingerprint
	var unparsed bool
	var normalized string
	var calls float64
	if err := owner.QueryRow(ctx, `select f.unparsed, f.normalized, sum(e.calls)
		from rotten.fingerprints f join rotten.events e on e.fingerprint_id = f.id
		where f.fingerprint = $1 group by f.unparsed, f.normalized`, wantFP).Scan(&unparsed, &normalized, &calls); err != nil {
		t.Fatalf("fallback fingerprint %s not stored: %v", wantFP, err)
	}
	if !unparsed || calls != 3 {
		t.Fatalf("stored unparsed=%v calls=%v, want true and 3", unparsed, calls)
	}
	if strings.Contains(normalized, "controller:users") || !strings.HasPrefix(normalized, "update unparsed_widgets") {
		t.Fatalf("stored text should be the pgss text without its trailing comment, got %q", normalized)
	}
	var contextCalls int64
	if err := owner.QueryRow(ctx, `select coalesce(sum(ec.c), 0)
		from rotten.event_context ec
		join rotten.events e on e.id = ec.event_id and e.observed_window_start = ec.observed_window_start
		join rotten.fingerprints f on f.id = e.fingerprint_id
		join rotten.controllers co on co.id = ec.controller_id
		join rotten.actions ac on ac.id = ec.action_id
		where f.fingerprint = $1 and co.controller = 'users' and ac.action = 'update'`, wantFP).Scan(&contextCalls); err != nil {
		t.Fatal(err)
	}
	if contextCalls != 3 {
		t.Fatalf("users#update context calls = %d, want 3", contextCalls)
	}
}
