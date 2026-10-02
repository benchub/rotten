package ingest_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	runningstat "github.com/benchub/runningstat"
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

var statsDomains = [...]string{
	"calls",
	"total_time",
	"min_time",
	"max_time",
	"mean_time",
	"stddev_time",
	"rows",
	"shared_blks_hit",
	"shared_blks_read",
	"shared_blks_dirtied",
	"shared_blks_written",
	"local_blks_hit",
	"local_blks_read",
	"local_blks_dirtied",
	"local_blks_written",
	"temp_blks_read",
	"temp_blks_written",
	"blk_read_time",
	"blk_write_time",
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

func harvestStatsRequest(logical, physical uint32, start time.Time, suffix, fp string, values ...float64) *connect.Request[rottenv1.SubmitHarvestRequest] {
	req := harvestRequest(logical, physical, start, suffix, fp)
	req.Msg.Aggregates = req.Msg.Aggregates[:0]
	for i, value := range values {
		req.Msg.Aggregates = append(req.Msg.Aggregates, statsAggregate(fp, value, fmt.Sprintf("select stats %d", i)))
	}
	return req
}

func statsAggregate(fp string, value float64, normalized string) *rottenv1.FingerprintAggregate {
	stddev := value * 6
	return &rottenv1.FingerprintAggregate{
		Fingerprint: fp,
		Normalized:  normalized,
		Contexts: []*rottenv1.QueryContext{
			{Controller: "stats", Action: "show", Count: 1},
		},
		Metrics: &rottenv1.Metrics{
			Calls:             uint64(value),
			TotalTime:         value * 2,
			MinTime:           value * 3,
			MaxTime:           value * 4,
			MeanTime:          value * 5,
			StddevTime:        &stddev,
			Rows:              uint64(value * 7),
			SharedBlksHit:     uint64(value * 8),
			SharedBlksRead:    uint64(value * 9),
			SharedBlksDirtied: uint64(value * 10),
			SharedBlksWritten: uint64(value * 11),
			LocalBlksHit:      uint64(value * 12),
			LocalBlksRead:     uint64(value * 13),
			LocalBlksDirtied:  uint64(value * 14),
			LocalBlksWritten:  uint64(value * 15),
			TempBlksRead:      uint64(value * 16),
			TempBlksWritten:   uint64(value * 17),
			BlkReadTime:       value * 18,
			BlkWriteTime:      value * 19,
		},
	}
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

func TestSubmitHarvestMergesFingerprintStatsWithWorkerValues(t *testing.T) {
	f := setupSubmit(t)
	start := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
	for i, value := range []float64{1, 2, 6} {
		req := harvestStatsRequest(f.reg.GetLogicalSourceId(), f.reg.GetPhysicalSourceId(), start.Add(time.Duration(i)*time.Minute), fmt.Sprintf("stats-%d", i), "fp-stats", value)
		resp, err := f.handler.SubmitHarvest(f.ctx, req)
		if err != nil {
			t.Fatalf("SubmitHarvest %d: %v", i, err)
		}
		if resp.Msg.GetStatus() != rottenv1.SubmitHarvestResponse_STATUS_ACCEPTED {
			t.Fatalf("status %d = %v, want ACCEPTED", i, resp.Msg.GetStatus())
		}
	}

	for _, source := range []uint32{0, f.reg.GetLogicalSourceId()} {
		checkSubmitStats(t, f.owner, "fp-stats", source, 3, 3, math.Sqrt(7), start.Add(2*time.Minute).Add(30*time.Second).Unix())
	}

	duplicate := harvestStatsRequest(f.reg.GetLogicalSourceId(), f.reg.GetPhysicalSourceId(), start, "stats-0", "fp-stats", 1)
	second, err := f.handler.SubmitHarvest(f.ctx, duplicate)
	if err != nil {
		t.Fatalf("duplicate SubmitHarvest: %v", err)
	}
	if second.Msg.GetStatus() != rottenv1.SubmitHarvestResponse_STATUS_DUPLICATE {
		t.Fatalf("duplicate status = %v, want DUPLICATE", second.Msg.GetStatus())
	}
	for _, source := range []uint32{0, f.reg.GetLogicalSourceId()} {
		checkSubmitStats(t, f.owner, "fp-stats", source, 3, 3, math.Sqrt(7), start.Add(2*time.Minute).Add(30*time.Second).Unix())
	}
}

func TestSubmitHarvestStatsBatchingMatchesAccumulated(t *testing.T) {
	f := setupSubmit(t)
	start := time.Now().UTC().Truncate(time.Second).Add(-2 * time.Hour)

	for i, value := range []float64{1, 2, 6} {
		req := harvestStatsRequest(f.reg.GetLogicalSourceId(), f.reg.GetPhysicalSourceId(), start.Add(time.Duration(i)*time.Minute), fmt.Sprintf("one-%d", i), "fp-one-at-a-time", value)
		if _, err := f.handler.SubmitHarvest(f.ctx, req); err != nil {
			t.Fatalf("one-at-a-time SubmitHarvest %d: %v", i, err)
		}
	}

	want := accumulatedSubmitStats(1, 2, 6)
	got := submitStatRows(t, f.owner, "fp-one-at-a-time", f.reg.GetLogicalSourceId())
	if !sameSubmitStats(got, want, false) {
		t.Fatalf("one-at-a-time stats = %s, want local accumulated stats %s", formatSubmitStats(got), formatSubmitStats(want))
	}
}

func TestSubmitHarvestRejectsDuplicateFingerprints(t *testing.T) {
	f := setupSubmit(t)
	start := time.Now().UTC().Truncate(time.Second).Add(-45 * time.Minute)
	req := harvestStatsRequest(f.reg.GetLogicalSourceId(), f.reg.GetPhysicalSourceId(), start, "duplicate-fingerprint", "fp-duplicate-fingerprint", 1, 2)

	_, err := f.handler.SubmitHarvest(f.ctx, req)
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("SubmitHarvest err = %v, want InvalidArgument", err)
	}
	if n := countSubmitRows(t, f.owner, "rotten.events"); n != 0 {
		t.Fatalf("events rows = %d, want 0", n)
	}
	if n := countSubmitRows(t, f.owner, "rotten.fingerprint_stats"); n != 0 {
		t.Fatalf("fingerprint_stats rows = %d, want 0", n)
	}
}

func TestSubmitHarvestStatsSkipsAbsentStddev(t *testing.T) {
	f := setupSubmit(t)
	start := time.Now().UTC().Truncate(time.Second).Add(-90 * time.Minute)
	first := harvestStatsRequest(f.reg.GetLogicalSourceId(), f.reg.GetPhysicalSourceId(), start, "stddev-present", "fp-absent-stddev", 4)
	if _, err := f.handler.SubmitHarvest(f.ctx, first); err != nil {
		t.Fatalf("present SubmitHarvest: %v", err)
	}

	second := harvestStatsRequest(f.reg.GetLogicalSourceId(), f.reg.GetPhysicalSourceId(), start.Add(time.Minute), "stddev-absent", "fp-absent-stddev", 8)
	second.Msg.Aggregates[0].Metrics.StddevTime = nil
	if _, err := f.handler.SubmitHarvest(f.ctx, second); err != nil {
		t.Fatalf("absent SubmitHarvest: %v", err)
	}

	got := submitStatRows(t, f.owner, "fp-absent-stddev", f.reg.GetLogicalSourceId())
	if r := got["stddev_time"]; r.count != 1 || !nearSubmit(r.mean, 24) || !nearSubmit(r.dev, 0) || r.last != start.Add(30*time.Second).Unix() {
		t.Fatalf("stddev_time after absent window = %+v, want first window only", r)
	}
	if r := got["calls"]; r.count != 2 || !nearSubmit(r.mean, 6) || !nearSubmit(r.dev, math.Sqrt(8)) || r.last != start.Add(time.Minute).Add(30*time.Second).Unix() {
		t.Fatalf("calls after absent stddev window = %+v, want both windows", r)
	}
}

func TestSubmitHarvestConcurrentFingerprintStatsDoNotDeadlock(t *testing.T) {
	f := setupSubmit(t)
	fixtures := []*submitFixture{f}
	for i := 1; i < 10; i++ {
		fixtures = append(fixtures, registerExtraSource(t, f, fmt.Sprintf("stats-worker-%d", i), fmt.Sprintf("db-submit-stats-%d.example", i), "submit", "test", "cluster", "primary"))
	}

	fingerprints := []string{"fp-concurrent-a", "fp-concurrent-b", "fp-concurrent-c"}
	base := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Hour)
	for iteration := range 5 {
		start := base.Add(time.Duration(iteration) * time.Minute)
		startSignal := make(chan struct{})
		errs := make(chan error, len(fixtures))
		var wg sync.WaitGroup
		for workerIndex, fixture := range fixtures {
			workerIndex := workerIndex
			fixture := fixture
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				req := harvestRequest(fixture.reg.GetLogicalSourceId(), fixture.reg.GetPhysicalSourceId(), start, fmt.Sprintf("concurrent-%d-%d", iteration, workerIndex))
				req.Msg.Aggregates = concurrentStatsAggregates(fingerprints, workerIndex)
				<-startSignal
				resp, err := fixture.handler.SubmitHarvest(auth.NewContext(ctx, fixture.key), req)
				if err != nil {
					errs <- err
					return
				}
				if resp.Msg.GetStatus() != rottenv1.SubmitHarvestResponse_STATUS_ACCEPTED {
					errs <- fmt.Errorf("status = %v, want ACCEPTED", resp.Msg.GetStatus())
					return
				}
			}()
		}
		close(startSignal)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent SubmitHarvest iteration %d: %v", iteration, err)
			}
		}
	}

	for _, fp := range fingerprints {
		for _, source := range []uint32{0, f.reg.GetLogicalSourceId()} {
			rows := submitStatRows(t, f.owner, fp, source)
			if len(rows) != len(statsDomains) {
				t.Fatalf("%s source %d rows = %d, want %d", fp, source, len(rows), len(statsDomains))
			}
			for i, domain := range statsDomains {
				r := rows[domain]
				wantMean := 10 * float64(i+1)
				if domain == "mean_time" {
					wantMean = 2
				}
				if r.count != 50 || !nearSubmit(r.mean, wantMean) || !nearSubmit(r.dev, 0) {
					t.Fatalf("%s source %d %s = count %d mean %v dev %v, want count 50 mean %v dev 0", fp, source, domain, r.count, r.mean, r.dev, wantMean)
				}
			}
		}
	}
}

func TestSubmitHarvestConcurrentStatsForExistingFingerprintDoNotRace(t *testing.T) {
	f := setupSubmit(t)
	fixtures := []*submitFixture{f}
	for i := 1; i < 10; i++ {
		fixtures = append(fixtures, registerExtraSource(t, f, fmt.Sprintf("race-worker-%d", i), fmt.Sprintf("db-submit-race-%d.example", i), "submit", "test", "cluster", "primary"))
	}

	base := time.Now().UTC().Truncate(time.Second).Add(-4 * time.Hour)
	for iteration := range 5 {
		fp := fmt.Sprintf("fp-existing-race-%d", iteration)
		precreateFingerprint(t, f.owner, fp)
		start := base.Add(time.Duration(iteration) * time.Minute)
		startSignal := make(chan struct{})
		errs := make(chan error, len(fixtures))
		var wg sync.WaitGroup
		for workerIndex, fixture := range fixtures {
			workerIndex := workerIndex
			fixture := fixture
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				req := harvestStatsRequest(fixture.reg.GetLogicalSourceId(), fixture.reg.GetPhysicalSourceId(), start, fmt.Sprintf("existing-race-%d-%d", iteration, workerIndex), fp, 10)
				<-startSignal
				resp, err := fixture.handler.SubmitHarvest(auth.NewContext(ctx, fixture.key), req)
				if err != nil {
					errs <- err
					return
				}
				if resp.Msg.GetStatus() != rottenv1.SubmitHarvestResponse_STATUS_ACCEPTED {
					errs <- fmt.Errorf("status = %v, want ACCEPTED", resp.Msg.GetStatus())
				}
			}()
		}
		close(startSignal)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent existing fingerprint iteration %d: %v", iteration, err)
			}
		}
		checkSubmitStats(t, f.owner, fp, f.reg.GetLogicalSourceId(), 10, 10, 0, start.Add(30*time.Second).Unix())
		checkSubmitStats(t, f.owner, fp, 0, 10, 10, 0, start.Add(30*time.Second).Unix())
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

type submitStatRow struct {
	count     int64
	mean, dev float64
	last      int64
}

func submitStatRows(t *testing.T, pool *pgxpool.Pool, fingerprint string, source uint32) map[string]submitStatRow {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		select fs.type::text, fs.count, fs.mean, fs.deviation, fs.last
		from rotten.fingerprint_stats fs
		join rotten.fingerprints f on f.id = fs.fingerprint_id
		where f.fingerprint = $1 and fs.logical_source_id = $2
		order by fs.type`, fingerprint, source)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]submitStatRow{}
	for rows.Next() {
		var domain string
		var r submitStatRow
		if err := rows.Scan(&domain, &r.count, &r.mean, &r.dev, &r.last); err != nil {
			t.Fatal(err)
		}
		got[domain] = r
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return got
}

func precreateFingerprint(t *testing.T, pool *pgxpool.Pool, fingerprint string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
		insert into rotten.fingerprints (fingerprint, normalized)
		values ($1, $2)`, fingerprint, "select precreated"); err != nil {
		t.Fatal(err)
	}
}

func accumulatedSubmitStats(values ...float64) map[string]submitStatRow {
	stats := make(map[string]*runningstat.RunningStat, len(statsDomains))
	for _, domain := range statsDomains {
		stats[domain] = &runningstat.RunningStat{}
	}
	for _, value := range values {
		for i, domain := range statsDomains {
			sample := value * float64(i+1)
			if domain == "mean_time" {
				sample = 2
			}
			stats[domain].Push(sample)
		}
	}
	rows := make(map[string]submitStatRow, len(statsDomains))
	for _, domain := range statsDomains {
		stat := stats[domain]
		rows[domain] = submitStatRow{
			count: stat.RunningStatCount(),
			mean:  stat.RunningStatMean(),
			dev:   stat.RunningStatDeviation(),
		}
	}
	return rows
}

func checkSubmitStats(t *testing.T, pool *pgxpool.Pool, fingerprint string, source uint32, count int64, mean, dev float64, last int64) {
	t.Helper()
	got := submitStatRows(t, pool, fingerprint, source)
	if len(got) != len(statsDomains) {
		t.Fatalf("source %d: %d fingerprint_stats rows, want %d", source, len(got), len(statsDomains))
	}
	for i, domain := range statsDomains {
		scale := float64(i + 1)
		wantMean := mean * scale
		wantDev := dev * scale
		if domain == "mean_time" {
			wantMean = 2
			wantDev = 0
		}
		r := got[domain]
		if r.count != count || !nearSubmit(r.mean, wantMean) || !nearSubmit(r.dev, wantDev) || r.last != last {
			t.Errorf("source %d %s = count %d mean %v dev %v last %d, want count %d mean %v dev %v last %d",
				source, domain, r.count, r.mean, r.dev, r.last, count, wantMean, wantDev, last)
		}
	}
}

func sameSubmitStats(a, b map[string]submitStatRow, compareLast bool) bool {
	if len(a) != len(b) {
		return false
	}
	for _, domain := range statsDomains {
		left, leftOK := a[domain]
		right, rightOK := b[domain]
		if !leftOK || !rightOK {
			return false
		}
		if left.count != right.count || !nearSubmit(left.mean, right.mean) || !nearSubmit(left.dev, right.dev) {
			return false
		}
		if compareLast && left.last != right.last {
			return false
		}
	}
	return true
}

func formatSubmitStats(rows map[string]submitStatRow) string {
	var b strings.Builder
	for _, domain := range statsDomains {
		r := rows[domain]
		fmt.Fprintf(&b, "%s:{count:%d mean:%g dev:%g last:%d} ", domain, r.count, r.mean, r.dev, r.last)
	}
	return b.String()
}

func nearSubmit(a, b float64) bool {
	return math.Abs(a-b) <= 1e-9*math.Max(1, math.Abs(b))
}

func concurrentStatsAggregates(fingerprints []string, workerIndex int) []*rottenv1.FingerprintAggregate {
	ordered := append([]string(nil), fingerprints...)
	if workerIndex%2 == 1 {
		for left, right := 0, len(ordered)-1; left < right; left, right = left+1, right-1 {
			ordered[left], ordered[right] = ordered[right], ordered[left]
		}
	} else {
		shift := workerIndex % len(ordered)
		ordered = append(ordered[shift:], ordered[:shift]...)
	}
	aggregates := make([]*rottenv1.FingerprintAggregate, 0, len(ordered))
	for _, fp := range ordered {
		aggregates = append(aggregates, statsAggregate(fp, 10, "select concurrent"))
	}
	return aggregates
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
