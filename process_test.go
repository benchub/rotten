package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// longInterval is passed as observation_interval. preRegister keeps
// processEvent from starting reportSamples at all, but if it ever does, the
// goroutine sleeps 2*longInterval seconds and stays out of the database.
const longInterval = 3600

// preRegister inserts the fingerprint row and registers a Fingerprint with a
// buffered channel, so processEvent takes the "already present" path and
// doesn't start consumeSamples and reportSamples. reportSamples reads f.last
// without the lock as it starts, which races with consumeSamples under -race
// (tasks 20261001-103222-10 and -13 handle that). It returns the channel
// processEvent sends its sample to.
func preRegister(t *testing.T, pool *pgxpool.Pool, fingerprint string) chan *Samples {
	t.Helper()
	var id uint64
	if err := pool.QueryRow(context.Background(),
		`insert into rotten.fingerprints (fingerprint, normalized) values ($1, $1) returning id`,
		fingerprint).Scan(&id); err != nil {
		t.Fatal(err)
	}
	ch := make(chan *Samples, 1)
	protectedFingerprints.Lock()
	protectedFingerprints.m[id] = &Fingerprint{db_id: id, samples: ch}
	protectedFingerprints.Unlock()
	return ch
}

// resetProcessGlobals clears the fingerprint registry and the processing
// counter now and at cleanup. Goroutines from earlier tests stay parked on
// their own channels and sleeps, and never see the new map. These are
// package globals, so tests that use them must not call t.Parallel().
func resetProcessGlobals(t *testing.T) {
	reset := func() {
		protectedFingerprints.Lock()
		protectedFingerprints.m = make(map[uint64]*Fingerprint)
		protectedFingerprints.Unlock()
		protectedProcessingCounter.Lock()
		protectedProcessingCounter.v = 0
		protectedProcessingCounter.Unlock()
	}
	reset()
	t.Cleanup(reset)
}

type processFixture struct {
	pool                       *pgxpool.Pool
	logical, physical          uint32
	controller, action, jobTag uint32
}

func startProcessDB(t *testing.T) processFixture {
	t.Helper()
	_, pool := startIdentityDB(t)
	resetProcessGlobals(t)
	f := processFixture{pool: pool}
	scan := func(q string, dst *uint32) {
		if err := pool.QueryRow(context.Background(), q).Scan(dst); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	scan(`insert into rotten.logical_sources (project,environment,cluster,role) values ('p','e','c','r') returning id`, &f.logical)
	scan(`insert into rotten.physical_sources (fqdn) values ('db1.example') returning id`, &f.physical)
	scan(`insert into rotten.controllers (controller) values ('users') returning id`, &f.controller)
	scan(`insert into rotten.actions (action) values ('show') returning id`, &f.action)
	scan(`insert into rotten.job_tags (job_tag) values ('Job#perform') returning id`, &f.jobTag)
	return f
}

// newEvent returns an event whose window falls inside the partitions that
// pg_partman makes around now.
func newEvent(query string, calls, total float64, ctxs map[string]uint32) *QueryEvent {
	start := time.Now().Add(-time.Minute).Unix()
	e := &QueryEvent{query: query, calls: calls, total_time: total, context: ctxs}
	e.observationTimeStart.sec = start
	e.observationTimeEnd.sec = start + 30
	return e
}

type contextRow struct {
	event         uint64
	ws, we        int64
	ctl, act, job *uint32
	c             uint32
}

func contextRows(t *testing.T, pool *pgxpool.Pool) []contextRow {
	t.Helper()
	rows, err := pool.Query(context.Background(), `select event_id,
		extract(epoch from observed_window_start)::bigint,
		extract(epoch from observed_window_end)::bigint,
		controller_id, action_id, job_tag_id, c
		from rotten.event_context order by c`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []contextRow
	for rows.Next() {
		var r contextRow
		if err := rows.Scan(&r.event, &r.ws, &r.we, &r.ctl, &r.act, &r.job, &r.c); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return got
}

func ptr(p *uint32) any {
	if p == nil {
		return nil
	}
	return *p
}

func TestProcessEventWritesEvent(t *testing.T) {
	f := startProcessDB(t)
	// An empty context map is the only way to get no event_context rows.
	e := newEvent("select 1", 7, 12.5, map[string]uint32{})
	samples := preRegister(t, f.pool, "select 1")
	processEvent(f.pool, f.logical, f.physical, longInterval, "select 1", e)

	if n := count(t, f.pool, "select count(*) from rotten.events"); n != 1 {
		t.Fatalf("events rows = %d, want 1", n)
	}
	var fpID, wantFP uint64
	var logical, physical uint32
	var ws, we int64
	var calls, tm float64
	err := f.pool.QueryRow(context.Background(), `select fingerprint_id, logical_source_id, physical_source_id,
		extract(epoch from observed_window_start)::bigint, extract(epoch from observed_window_end)::bigint,
		calls, time from rotten.events`).Scan(&fpID, &logical, &physical, &ws, &we, &calls, &tm)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.pool.QueryRow(context.Background(), `select id from rotten.fingerprints where fingerprint='select 1'`).Scan(&wantFP); err != nil {
		t.Fatalf("fingerprint row: %v", err)
	}
	if fpID != wantFP || logical != f.logical || physical != f.physical {
		t.Errorf("ids = (%d,%d,%d), want (%d,%d,%d)", fpID, logical, physical, wantFP, f.logical, f.physical)
	}
	if ws != e.observationTimeStart.sec || we != e.observationTimeEnd.sec {
		t.Errorf("window = [%d,%d], want [%d,%d]", ws, we, e.observationTimeStart.sec, e.observationTimeEnd.sec)
	}
	if calls != 7 || tm != 12.5 {
		t.Errorf("calls,time = %v,%v, want 7,12.5", calls, tm)
	}
	if n := count(t, f.pool, "select count(*) from rotten.event_context"); n != 0 {
		t.Errorf("empty context map wrote %d event_context rows, want 0", n)
	}
	select {
	case s := <-samples:
		if s.metrics["calls"] != 7 || s.metrics["total_time"] != 12.5 {
			t.Errorf("sample calls,total_time = %v,%v, want 7,12.5", s.metrics["calls"], s.metrics["total_time"])
		}
	default:
		t.Error("processEvent sent no sample to the existing fingerprint")
	}
	if v := stillProcessing(); v != 0 {
		t.Errorf("stillProcessing = %d after success, want 0", v)
	}
}

func TestProcessEventWritesContexts(t *testing.T) {
	f := startProcessDB(t)
	// Hashes use main's format: no separators, and "" when the query had no
	// marginalia. A zero count is skipped.
	all := fmt.Sprintf("controller:%daction:%djob_tag:%d", f.controller, f.action, f.jobTag)
	job := fmt.Sprintf("job_tag:%d", f.jobTag)
	e := newEvent("select 2", 12, 3, map[string]uint32{"": 2, job: 3, all: 7, "controller:999": 0})
	preRegister(t, f.pool, "select 2")
	processEvent(f.pool, f.logical, f.physical, longInterval, "select 2", e)

	var eventID uint64
	if err := f.pool.QueryRow(context.Background(), `select id from rotten.events`).Scan(&eventID); err != nil {
		t.Fatal(err)
	}
	got := contextRows(t, f.pool)
	if len(got) != 3 {
		t.Fatalf("event_context rows = %d, want 3 (one per nonzero hash)", len(got))
	}
	for _, r := range got {
		if r.event != eventID || r.ws != e.observationTimeStart.sec || r.we != e.observationTimeEnd.sec {
			t.Errorf("row c=%d: event %d window [%d,%d], want event %d window [%d,%d]",
				r.c, r.event, r.ws, r.we, eventID, e.observationTimeStart.sec, e.observationTimeEnd.sec)
		}
	}
	want := []struct {
		c             uint32
		ctl, act, job any
	}{
		{2, nil, nil, nil},
		{3, nil, nil, f.jobTag},
		{7, f.controller, f.action, f.jobTag},
	}
	for i, w := range want {
		r := got[i]
		if r.c != w.c || ptr(r.ctl) != w.ctl || ptr(r.act) != w.act || ptr(r.job) != w.job {
			t.Errorf("row %d = c %d ctl %v act %v job %v, want c %d ctl %v act %v job %v",
				i, r.c, ptr(r.ctl), ptr(r.act), ptr(r.job), w.c, w.ctl, w.act, w.job)
		}
	}
	if v := stillProcessing(); v != 0 {
		t.Errorf("stillProcessing = %d after success, want 0", v)
	}
}

func TestProcessEventEarlyReturnReleasesCounter(t *testing.T) {
	f := startProcessDB(t)
	// pg_query.Normalize rejects this, so processEvent returns before the
	// transaction starts.
	e := newEvent("selec oops (", 1, 1, map[string]uint32{"": 1})
	processEvent(f.pool, f.logical, f.physical, longInterval, "selec oops (", e)

	if n := count(t, f.pool, "select count(*) from rotten.events"); n != 0 {
		t.Errorf("unparseable query wrote %d events, want 0", n)
	}
	if v := stillProcessing(); v != 0 {
		t.Errorf("stillProcessing = %d after early return, want 0", v)
	}
}
