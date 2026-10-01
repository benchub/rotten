package main

import (
	"math"
	"testing"
)

// These tests characterize mergeEvent as it behaves today. Some of the
// expected values encode known oddities (see the comments), not correct
// statistics. Don't "fix" an expected value without a backlog task.

func approx(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("%s = %.12f, want %.12f", name, got, want)
	}
}

func TestMergeEventSumsMinMax(t *testing.T) {
	a := QueryEvent{
		query: "a", calls: 2, total_time: 20, min_time: 3, max_time: 15,
		mean_time: 10, stddev_time: 2,
		rows: 1, shared_blks_hit: 2, shared_blks_read: 3, shared_blks_dirtied: 4,
		shared_blks_written: 5, local_blks_hit: 6, local_blks_read: 7,
		local_blks_dirtied: 8, local_blks_written: 9, temp_blks_read: 10,
		temp_blks_written: 11, blk_read_time: 12, blk_write_time: 13,
		context:              map[string]uint32{"": 2},
		observationTimeStart: PoorMansTime{100}, observationTimeEnd: PoorMansTime{200},
	}
	b := QueryEvent{
		query: "b", calls: 2, total_time: 40, min_time: 1, max_time: 30,
		mean_time: 20, stddev_time: 4,
		rows: 100, shared_blks_hit: 200, shared_blks_read: 300, shared_blks_dirtied: 400,
		shared_blks_written: 500, local_blks_hit: 600, local_blks_read: 700,
		local_blks_dirtied: 800, local_blks_written: 900, temp_blks_read: 1000,
		temp_blks_written: 1100, blk_read_time: 1200, blk_write_time: 1300,
		context:              map[string]uint32{"controller:1": 2},
		observationTimeStart: PoorMansTime{999}, observationTimeEnd: PoorMansTime{999},
	}

	m := mergeEvent(a, b)

	if m.query != "a" {
		t.Errorf("query = %q, want the first event's query", m.query)
	}
	if m.observationTimeStart.sec != 100 || m.observationTimeEnd.sec != 200 {
		t.Errorf("window = %d-%d, want the first event's 100-200", m.observationTimeStart.sec, m.observationTimeEnd.sec)
	}
	for _, c := range []struct {
		name      string
		got, want float64
	}{
		{"calls", m.calls, 4},
		{"total_time", m.total_time, 60},
		{"min_time", m.min_time, 1},
		{"max_time", m.max_time, 30},
		{"rows", m.rows, 101},
		{"shared_blks_hit", m.shared_blks_hit, 202},
		{"shared_blks_read", m.shared_blks_read, 303},
		{"shared_blks_dirtied", m.shared_blks_dirtied, 404},
		{"shared_blks_written", m.shared_blks_written, 505},
		{"local_blks_hit", m.local_blks_hit, 606},
		{"local_blks_read", m.local_blks_read, 707},
		{"local_blks_dirtied", m.local_blks_dirtied, 808},
		{"local_blks_written", m.local_blks_written, 909},
		{"temp_blks_read", m.temp_blks_read, 1010},
		{"temp_blks_written", m.temp_blks_written, 1111},
		{"blk_read_time", m.blk_read_time, 1212},
		{"blk_write_time", m.blk_write_time, 1313},
	} {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}

	// min and max keep the first event's value when it's already the extreme.
	m2 := mergeEvent(QueryEvent{calls: 1, min_time: 1, max_time: 30, context: map[string]uint32{}},
		QueryEvent{calls: 1, min_time: 3, max_time: 15, context: map[string]uint32{}})
	if m2.min_time != 1 || m2.max_time != 30 {
		t.Errorf("min/max = %v/%v, want 1/30", m2.min_time, m2.max_time)
	}
}

// Hand-computed with runningstat v0.2.0 math. Init(n, mean, sd) sets
// S = sd^2*(n-1) when n > 1. When n <= 1, Init stores sd unsquared in
// m_newS but sets m_oldS to 0. Merge reads the first side's m_newS and the
// second side's m_oldS, so an unsquared sd only matters on the first side.
// On the second side, sd is dropped. Merge gives
// n = n1+n2, mean = m1 + n2*(m2-m1)/n, S = S1 + S2 + n1*n2*(m2-m1)^2/n,
// and stddev = sqrt(S/(n-1)).
//
// Oddity: calls is summed before the first RunningStat is built, so the
// first side is weighted by a.calls+b.calls instead of a.calls.
func TestMergeEventRunningStat(t *testing.T) {
	t.Run("counts above one", func(t *testing.T) {
		a := QueryEvent{calls: 2, mean_time: 10, stddev_time: 2, context: map[string]uint32{}}
		b := QueryEvent{calls: 2, mean_time: 20, stddev_time: 4, context: map[string]uint32{}}
		m := mergeEvent(a, b)
		// rs1 = Init(4, 10, 2): S1 = 4*3 = 12.
		// rs2 = Init(2, 20, 4): S2 = 16*1 = 16.
		// n = 6, delta = 10, mean = 10 + 2*10/6 = 40/3.
		// S = 12 + 16 + 4*2*100/6 = 28 + 400/3 = 484/3. var = 484/15.
		approx(t, "mean_time", m.mean_time, 40.0/3)
		approx(t, "stddev_time", m.stddev_time, math.Sqrt(484.0/15))
		// For contrast, weighting by the real counts would give mean 15 and
		// stddev sqrt(40). This test locks in today's behavior.
	})

	t.Run("single calls", func(t *testing.T) {
		a := QueryEvent{calls: 1, mean_time: 5, stddev_time: 0, context: map[string]uint32{}}
		b := QueryEvent{calls: 1, mean_time: 9, stddev_time: 0, context: map[string]uint32{}}
		m := mergeEvent(a, b)
		// rs1 = Init(2, 5, 0): S1 = 0. rs2 = Init(1, 9, 0): S2 = 0.
		// n = 3, delta = 4, mean = 5 + 4/3 = 19/3.
		// S = 2*1*16/3 = 32/3. var = 16/3.
		approx(t, "mean_time", m.mean_time, 19.0/3)
		approx(t, "stddev_time", m.stddev_time, math.Sqrt(16.0/3))
	})

	t.Run("second side with one call drops its stddev", func(t *testing.T) {
		a := QueryEvent{calls: 1, mean_time: 5, stddev_time: 0, context: map[string]uint32{}}
		b := QueryEvent{calls: 1, mean_time: 9, stddev_time: 3, context: map[string]uint32{}}
		m := mergeEvent(a, b)
		// rs2 = Init(1, 9, 3): m_oldS = 0, so b's sd of 3 never reaches Merge.
		// The result matches the "single calls" case: mean 19/3, var 16/3.
		approx(t, "mean_time", m.mean_time, 19.0/3)
		approx(t, "stddev_time", m.stddev_time, math.Sqrt(16.0/3))
	})

	t.Run("fractional calls truncate in Init", func(t *testing.T) {
		a := QueryEvent{calls: 1.5, mean_time: 4, stddev_time: 0, context: map[string]uint32{}}
		b := QueryEvent{calls: 1.9, mean_time: 10, stddev_time: 0, context: map[string]uint32{}}
		m := mergeEvent(a, b)
		// calls = 3.4. rs1 = Init(int64(3.4)=3, 4, 0). rs2 = Init(int64(1.9)=1, 10, 0).
		// n = 4, delta = 6, mean = 4 + 6/4 = 5.5. S = 3*1*36/4 = 27. var = 9.
		approx(t, "calls", m.calls, 3.4)
		approx(t, "mean_time", m.mean_time, 5.5)
		approx(t, "stddev_time", m.stddev_time, 3)
	})
}

func TestMergeEventContextHistogram(t *testing.T) {
	ctx := map[string]uint32{"": 3, "controller:1": 5}
	a := QueryEvent{calls: 8, context: ctx}

	m := mergeEvent(a, QueryEvent{calls: 2, context: map[string]uint32{"controller:1": 2}})
	if got := m.context["controller:1"]; got != 7 {
		t.Errorf("existing hash count = %d, want 7", got)
	}
	m = mergeEvent(m, QueryEvent{calls: 4, context: map[string]uint32{"controller:2action:3": 4}})
	if got := m.context["controller:2action:3"]; got != 4 {
		t.Errorf("new hash count = %d, want 4", got)
	}
	if got := m.context[""]; got != 3 {
		t.Errorf("untouched hash count = %d, want 3", got)
	}
	if len(m.context) != 3 {
		t.Errorf("context = %v, want three hashes", m.context)
	}
	// Oddity: the merged event shares the first event's map, so the merge
	// mutates a.context in place.
	if ctx["controller:1"] != 7 {
		t.Errorf("a.context wasn't mutated in place: %v", ctx)
	}
}

// Each side's histogram is built from uint32(calls) before merging, so a
// fractional call count gets truncated there, while calls keeps the fraction.
func TestMergeEventContextTruncatedCalls(t *testing.T) {
	b := QueryEvent{calls: 3.9}
	b.context = map[string]uint32{"job_tag:7": uint32(b.calls)}
	m := mergeEvent(QueryEvent{calls: 1, context: map[string]uint32{"job_tag:7": 1}}, b)
	if got := m.context["job_tag:7"]; got != 4 {
		t.Errorf("merged count = %d, want 4", got)
	}
	approx(t, "calls", m.calls, 4.9)
}
