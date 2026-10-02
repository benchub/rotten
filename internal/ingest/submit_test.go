package ingest_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	"github.com/benchub/rotten/internal/auth"
	"github.com/benchub/rotten/internal/ingest"
	"github.com/benchub/rotten/internal/testdb"
)

type submitFixture struct {
	ctx     context.Context
	owner   *pgxpool.Pool
	ingest  *pgxpool.Pool
	handler *ingest.Handler
	logs    *bytes.Buffer
	key     auth.Key
	reg     *rottenv1.RegisterResponse
}

func setupSubmit(t *testing.T) *submitFixture {
	t.Helper()
	db := testdb.StartRotten(t)
	ctx := context.Background()
	owner, err := pgxpool.New(ctx, db.DSNAs(t, testdb.OwnerRole))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(owner.Close)
	ingestPool, err := pgxpool.New(ctx, db.DSNAs(t, testdb.IngestRole))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ingestPool.Close)
	created, err := auth.CreateKey(ctx, owner, "submit-worker", "db-submit.example", "test")
	if err != nil {
		t.Fatal(err)
	}
	id, _, err := auth.ParseKey(created.Token)
	if err != nil {
		t.Fatal(err)
	}
	key := auth.Key{ID: id, Name: "submit-worker", FQDN: "db-submit.example"}
	logs := &bytes.Buffer{}
	handler := ingest.NewHandler(ingestPool, ingest.Options{
		Logger: slog.New(slog.NewTextHandler(logs, nil)),
	})
	reg, err := handler.Register(auth.NewContext(ctx, key), registerRequest("submit", "test", "cluster", "primary", "db-submit.example"))
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	return &submitFixture{
		ctx:     auth.NewContext(ctx, key),
		owner:   owner,
		ingest:  ingestPool,
		handler: handler,
		logs:    logs,
		key:     key,
		reg:     reg.Msg,
	}
}

func harvestRequest(logical, physical uint32, start time.Time, suffix string, fps ...string) *connect.Request[rottenv1.SubmitHarvestRequest] {
	if len(fps) == 0 {
		fps = []string{"fp-" + suffix}
	}
	aggs := make([]*rottenv1.FingerprintAggregate, 0, len(fps))
	for i, fp := range fps {
		calls := uint64(12 + i)
		aggs = append(aggs, &rottenv1.FingerprintAggregate{
			Fingerprint: fp,
			Normalized:  "select $" + fmt.Sprint(i+1),
			Contexts: []*rottenv1.QueryContext{
				{Controller: "users", Action: "show", JobTag: "UserJob#perform", Count: 7},
				{JobTag: "UserJob#perform", Count: 3},
				{Count: 2},
			},
			Metrics: &rottenv1.Metrics{
				Calls:     calls,
				TotalTime: float64(calls) * 2.5,
				MinTime:   1,
				MaxTime:   5,
				MeanTime:  2.5,
				Rows:      calls * 10,
			},
		})
	}
	end := start.Add(30 * time.Second)
	msg := &rottenv1.SubmitHarvestRequest{
		LogicalSourceId:  logical,
		PhysicalSourceId: physical,
		WindowStart:      timestamppb.New(start),
		WindowEnd:        timestamppb.New(end),
		Aggregates:       aggs,
	}
	msg.BatchId = fmt.Sprintf("%d:%d:%d", physical, start.UnixMicro(), end.UnixMicro())
	return connect.NewRequest(msg)
}

type storedContext struct {
	Controller        *string
	Action            *string
	JobTag            *string
	WindowStartMicros int64
	WindowEndMicros   int64
	Count             int
}

type storedEvent struct {
	Fingerprint       string
	Normalized        string
	LogicalSourceID   uint32
	PhysicalSourceID  uint32
	WindowStartMicros int64
	WindowEndMicros   int64
	Calls             float64
	Time              float64
	Contexts          []storedContext
}

func storedEvents(t *testing.T, pool *pgxpool.Pool) []storedEvent {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		select e.id, f.fingerprint, f.normalized, e.logical_source_id, e.physical_source_id,
			extract(epoch from e.observed_window_start) * 1000000,
			extract(epoch from e.observed_window_end) * 1000000,
			e.calls, e.time
		from rotten.events e
		join rotten.fingerprints f on f.id = e.fingerprint_id
		order by e.id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []storedEvent
	var eventIDs []int64
	for rows.Next() {
		var id int64
		var e storedEvent
		if err := rows.Scan(&id, &e.Fingerprint, &e.Normalized, &e.LogicalSourceID, &e.PhysicalSourceID, &e.WindowStartMicros, &e.WindowEndMicros, &e.Calls, &e.Time); err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
		eventIDs = append(eventIDs, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for i, id := range eventIDs {
		ctxRows, err := pool.Query(context.Background(), `
			select c.controller, a.action, j.job_tag,
				extract(epoch from ec.observed_window_start) * 1000000,
				extract(epoch from ec.observed_window_end) * 1000000,
				ec.c
			from rotten.event_context ec
			left join rotten.controllers c on c.id = ec.controller_id
			left join rotten.actions a on a.id = ec.action_id
			left join rotten.job_tags j on j.id = ec.job_tag_id
			where ec.event_id = $1
			order by ec.c`, id)
		if err != nil {
			t.Fatal(err)
		}
		var contexts []storedContext
		for ctxRows.Next() {
			var c storedContext
			if err := ctxRows.Scan(&c.Controller, &c.Action, &c.JobTag, &c.WindowStartMicros, &c.WindowEndMicros, &c.Count); err != nil {
				t.Fatal(err)
			}
			contexts = append(contexts, c)
		}
		if err := ctxRows.Err(); err != nil {
			t.Fatal(err)
		}
		ctxRows.Close()
		out[i].Contexts = contexts
	}
	return out
}

func TestSubmitHarvestWritesExpectedRowsAndDedupes(t *testing.T) {
	f := setupSubmit(t)
	start := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)
	req := harvestRequest(f.reg.GetLogicalSourceId(), f.reg.GetPhysicalSourceId(), start, "same", "fp-same")

	first, err := f.handler.SubmitHarvest(f.ctx, req)
	if err != nil {
		t.Fatalf("first SubmitHarvest: %v", err)
	}
	if first.Msg.GetStatus() != rottenv1.SubmitHarvestResponse_STATUS_ACCEPTED {
		t.Fatalf("first status = %v, want ACCEPTED", first.Msg.GetStatus())
	}
	second, err := f.handler.SubmitHarvest(f.ctx, req)
	if err != nil {
		t.Fatalf("second SubmitHarvest: %v", err)
	}
	if second.Msg.GetStatus() != rottenv1.SubmitHarvestResponse_STATUS_DUPLICATE {
		t.Fatalf("second status = %v, want DUPLICATE", second.Msg.GetStatus())
	}

	got := storedEvents(t, f.owner)
	if len(got) != 1 {
		t.Fatalf("stored events = %d, want 1: %+v", len(got), got)
	}
	if got[0].Fingerprint != "fp-same" ||
		got[0].Normalized != "select $1" ||
		got[0].LogicalSourceID != f.reg.GetLogicalSourceId() ||
		got[0].PhysicalSourceID != f.reg.GetPhysicalSourceId() ||
		got[0].WindowStartMicros != start.UnixMicro() ||
		got[0].WindowEndMicros != start.Add(30*time.Second).UnixMicro() ||
		got[0].Calls != 12 ||
		got[0].Time != 30 {
		t.Fatalf("event = %+v, want fp/logical/physical/window/counters from request", got[0])
	}
	wantContexts := []storedContext{
		{WindowStartMicros: start.UnixMicro(), WindowEndMicros: start.Add(30 * time.Second).UnixMicro(), Count: 2},
		{JobTag: strPtr("UserJob#perform"), WindowStartMicros: start.UnixMicro(), WindowEndMicros: start.Add(30 * time.Second).UnixMicro(), Count: 3},
		{Controller: strPtr("users"), Action: strPtr("show"), JobTag: strPtr("UserJob#perform"), WindowStartMicros: start.UnixMicro(), WindowEndMicros: start.Add(30 * time.Second).UnixMicro(), Count: 7},
	}
	if !sameContexts(got[0].Contexts, wantContexts) {
		t.Fatalf("contexts = %s, want %s", formatContexts(got[0].Contexts), formatContexts(wantContexts))
	}
	var batches int
	if err := f.owner.QueryRow(context.Background(), "select count(*) from rotten.ingested_batches").Scan(&batches); err != nil {
		t.Fatal(err)
	}
	if batches != 1 {
		t.Fatalf("ingested_batches rows = %d, want 1", batches)
	}
}

func TestSubmitHarvestRejectsMismatchedSourcePairAndKey(t *testing.T) {
	f := setupSubmit(t)
	var otherLogical, otherPhysical uint32
	if err := f.owner.QueryRow(context.Background(), `
		insert into rotten.logical_sources(project, environment, cluster, role)
		values ('other', 'test', 'cluster', 'primary') returning id`).Scan(&otherLogical); err != nil {
		t.Fatal(err)
	}
	if err := f.owner.QueryRow(context.Background(), `
		insert into rotten.physical_sources(fqdn)
		values ('other-db.example') returning id`).Scan(&otherPhysical); err != nil {
		t.Fatal(err)
	}
	if _, err := f.owner.Exec(context.Background(), `
		insert into rotten.logical_physical_sources(logical_source_id, physical_source_id)
		values ($1, $2)`, otherLogical, otherPhysical); err != nil {
		t.Fatal(err)
	}
	req := harvestRequest(f.reg.GetLogicalSourceId(), otherPhysical, time.Now().UTC().Add(-time.Minute), "mismatch")

	_, err := f.handler.SubmitHarvest(f.ctx, req)
	if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("SubmitHarvest err = %v, want PermissionDenied", err)
	}
	if n := countSubmitRows(t, f.owner, "rotten.events"); n != 0 {
		t.Fatalf("events rows = %d, want 0", n)
	}
}

func TestSubmitHarvestRejectsOverlappingNewWindow(t *testing.T) {
	f := setupSubmit(t)
	start := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)
	if _, err := f.handler.SubmitHarvest(f.ctx, harvestRequest(f.reg.GetLogicalSourceId(), f.reg.GetPhysicalSourceId(), start, "first")); err != nil {
		t.Fatalf("first SubmitHarvest: %v", err)
	}

	_, err := f.handler.SubmitHarvest(f.ctx, harvestRequest(f.reg.GetLogicalSourceId(), f.reg.GetPhysicalSourceId(), start.Add(15*time.Second), "overlap"))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("overlap SubmitHarvest err = %v, want FailedPrecondition", err)
	}
	if n := countSubmitRows(t, f.owner, "rotten.events"); n != 1 {
		t.Fatalf("events rows = %d, want only the first event", n)
	}
}

func TestSubmitHarvestOverlapIsScopedToPhysicalSource(t *testing.T) {
	f := setupSubmit(t)
	second := registerExtraSource(t, f, "replica-worker", "db-submit-replica.example", "submit", "test", "cluster", "primary")
	start := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)

	if _, err := f.handler.SubmitHarvest(f.ctx, harvestRequest(f.reg.GetLogicalSourceId(), f.reg.GetPhysicalSourceId(), start, "primary")); err != nil {
		t.Fatalf("primary SubmitHarvest: %v", err)
	}
	resp, err := f.handler.SubmitHarvest(second.ctx, harvestRequest(second.reg.GetLogicalSourceId(), second.reg.GetPhysicalSourceId(), start.Add(3*time.Second), "replica"))
	if err != nil {
		t.Fatalf("replica shifted SubmitHarvest: %v", err)
	}
	if resp.Msg.GetStatus() != rottenv1.SubmitHarvestResponse_STATUS_ACCEPTED {
		t.Fatalf("replica status = %v, want ACCEPTED", resp.Msg.GetStatus())
	}

	_, err = f.handler.SubmitHarvest(f.ctx, harvestRequest(f.reg.GetLogicalSourceId(), f.reg.GetPhysicalSourceId(), start.Add(6*time.Second), "same-physical-overlap"))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("same physical overlap err = %v, want FailedPrecondition", err)
	}
	if n := countSubmitRows(t, f.owner, "rotten.events"); n != 2 {
		t.Fatalf("events rows = %d, want primary and replica rows only", n)
	}
}

func TestSubmitHarvestConcurrentNewFingerprints(t *testing.T) {
	f := setupSubmit(t)
	second := registerExtraSource(t, f, "submit-worker-2", "db-submit-2.example", "submit2", "test", "cluster", "primary")

	for i := range 12 {
		start := time.Now().UTC().Truncate(time.Second).Add(time.Duration(-10-i) * time.Minute)
		left := harvestRequest(f.reg.GetLogicalSourceId(), f.reg.GetPhysicalSourceId(), start, fmt.Sprintf("left-%d", i), fmt.Sprintf("fp-deadlock-x-%d", i), fmt.Sprintf("fp-deadlock-y-%d", i))
		right := harvestRequest(second.reg.GetLogicalSourceId(), second.reg.GetPhysicalSourceId(), start, fmt.Sprintf("right-%d", i), fmt.Sprintf("fp-deadlock-y-%d", i), fmt.Sprintf("fp-deadlock-x-%d", i))
		left.Msg.Aggregates[0].Contexts[0].Controller = fmt.Sprintf("ctl-x-%d", i)
		left.Msg.Aggregates[1].Contexts[0].Controller = fmt.Sprintf("ctl-y-%d", i)
		right.Msg.Aggregates[0].Contexts[0].Controller = fmt.Sprintf("ctl-y-%d", i)
		right.Msg.Aggregates[1].Contexts[0].Controller = fmt.Sprintf("ctl-x-%d", i)
		left.Msg.Aggregates[0].Contexts[0].Action = fmt.Sprintf("act-x-%d", i)
		left.Msg.Aggregates[1].Contexts[0].Action = fmt.Sprintf("act-y-%d", i)
		right.Msg.Aggregates[0].Contexts[0].Action = fmt.Sprintf("act-y-%d", i)
		right.Msg.Aggregates[1].Contexts[0].Action = fmt.Sprintf("act-x-%d", i)
		left.Msg.Aggregates[0].Contexts[0].JobTag = fmt.Sprintf("job-x-%d", i)
		left.Msg.Aggregates[1].Contexts[0].JobTag = fmt.Sprintf("job-y-%d", i)
		right.Msg.Aggregates[0].Contexts[0].JobTag = fmt.Sprintf("job-y-%d", i)
		right.Msg.Aggregates[1].Contexts[0].JobTag = fmt.Sprintf("job-x-%d", i)
		runConcurrentHarvests(t, left, f.ctx, f.handler, right, second.ctx, second.handler)
	}
	if n := countSubmitRows(t, f.owner, "rotten.events"); n != 48 {
		t.Fatalf("events rows = %d, want 48", n)
	}
	if n := countSubmitRows(t, f.owner, "rotten.fingerprints"); n != 24 {
		t.Fatalf("fingerprints rows = %d, want 24 shared IDs", n)
	}
}

func TestSubmitHarvestDuplicateDifferentContentWarns(t *testing.T) {
	f := setupSubmit(t)
	start := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)
	req := harvestRequest(f.reg.GetLogicalSourceId(), f.reg.GetPhysicalSourceId(), start, "same", "fp-same")
	if _, err := f.handler.SubmitHarvest(f.ctx, req); err != nil {
		t.Fatalf("first SubmitHarvest: %v", err)
	}
	changed := harvestRequest(f.reg.GetLogicalSourceId(), f.reg.GetPhysicalSourceId(), start, "same", "fp-different")
	resp, err := f.handler.SubmitHarvest(f.ctx, changed)
	if err != nil {
		t.Fatalf("changed duplicate SubmitHarvest: %v", err)
	}
	if resp.Msg.GetStatus() != rottenv1.SubmitHarvestResponse_STATUS_DUPLICATE {
		t.Fatalf("status = %v, want DUPLICATE", resp.Msg.GetStatus())
	}
	if !strings.Contains(f.logs.String(), "duplicate batch_id with different content") {
		t.Fatalf("logs = %q, want different-content warning", f.logs.String())
	}
}

func strPtr(s string) *string { return &s }

func registerExtraSource(t *testing.T, f *submitFixture, name, fqdn, project, environment, cluster, role string) *submitFixture {
	t.Helper()
	created, err := auth.CreateKey(context.Background(), f.owner, name, fqdn, "test")
	if err != nil {
		t.Fatal(err)
	}
	id, _, err := auth.ParseKey(created.Token)
	if err != nil {
		t.Fatal(err)
	}
	key := auth.Key{ID: id, Name: name, FQDN: fqdn}
	reg, err := f.handler.Register(auth.NewContext(context.Background(), key), registerRequest(project, environment, cluster, role, fqdn))
	if err != nil {
		t.Fatalf("Register extra source: %v", err)
	}
	return &submitFixture{
		ctx:     auth.NewContext(context.Background(), key),
		owner:   f.owner,
		ingest:  f.ingest,
		handler: f.handler,
		logs:    f.logs,
		key:     key,
		reg:     reg.Msg,
	}
}

func runConcurrentHarvests(t *testing.T, left *connect.Request[rottenv1.SubmitHarvestRequest], leftCtx context.Context, leftHandler *ingest.Handler, right *connect.Request[rottenv1.SubmitHarvestRequest], rightCtx context.Context, rightHandler *ingest.Handler) {
	t.Helper()
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, work := range []struct {
		ctx     context.Context
		handler *ingest.Handler
		req     *connect.Request[rottenv1.SubmitHarvestRequest]
	}{
		{leftCtx, leftHandler, left},
		{rightCtx, rightHandler, right},
	} {
		work := work
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := work.handler.SubmitHarvest(work.ctx, work.req)
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("SubmitHarvest: %v", err)
		}
	}
}

func sameContexts(a, b []storedContext) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if strValue(a[i].Controller) != strValue(b[i].Controller) ||
			strValue(a[i].Action) != strValue(b[i].Action) ||
			strValue(a[i].JobTag) != strValue(b[i].JobTag) ||
			a[i].WindowStartMicros != b[i].WindowStartMicros ||
			a[i].WindowEndMicros != b[i].WindowEndMicros ||
			a[i].Count != b[i].Count {
			return false
		}
	}
	return true
}

func formatContexts(contexts []storedContext) string {
	var b strings.Builder
	for _, c := range contexts {
		fmt.Fprintf(&b, "{controller:%q action:%q job:%q window:[%d,%d] count:%d}", strValue(c.Controller), strValue(c.Action), strValue(c.JobTag), c.WindowStartMicros, c.WindowEndMicros, c.Count)
	}
	return b.String()
}

func strValue(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func countSubmitRows(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "select count(*) from "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
