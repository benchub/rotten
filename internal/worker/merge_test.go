package worker

import (
	"math"
	"testing"

	"github.com/benchub/rotten/internal/harvestlimits"
)

// These tests characterize mergeEvent as it behaves today.

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
		context:              map[string]uint64{"": 2},
		observationTimeStart: PoorMansTime{100}, observationTimeEnd: PoorMansTime{200},
	}
	b := QueryEvent{
		query: "b", calls: 2, total_time: 40, min_time: 1, max_time: 30,
		mean_time: 20, stddev_time: 4,
		rows: 100, shared_blks_hit: 200, shared_blks_read: 300, shared_blks_dirtied: 400,
		shared_blks_written: 500, local_blks_hit: 600, local_blks_read: 700,
		local_blks_dirtied: 800, local_blks_written: 900, temp_blks_read: 1000,
		temp_blks_written: 1100, blk_read_time: 1200, blk_write_time: 1300,
		context:              map[string]uint64{"controller:1": 2},
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
	m2 := mergeEvent(QueryEvent{calls: 1, min_time: 1, max_time: 30, context: map[string]uint64{}},
		QueryEvent{calls: 1, min_time: 3, max_time: 15, context: map[string]uint64{}})
	if m2.min_time != 1 || m2.max_time != 30 {
		t.Errorf("min/max = %v/%v, want 1/30", m2.min_time, m2.max_time)
	}
}

func TestMergeEventRunningStat(t *testing.T) {
	t.Run("counts above one", func(t *testing.T) {
		a := QueryEvent{calls: 2, mean_time: 10, stddev_time: 2, context: map[string]uint64{}}
		b := QueryEvent{calls: 2, mean_time: 20, stddev_time: 4, context: map[string]uint64{}}
		m := mergeEvent(a, b)
		approx(t, "mean_time", m.mean_time, 15)
		approx(t, "stddev_time", m.stddev_time, math.Sqrt(35))
	})

	t.Run("single calls", func(t *testing.T) {
		a := QueryEvent{calls: 1, mean_time: 5, stddev_time: 0, context: map[string]uint64{}}
		b := QueryEvent{calls: 1, mean_time: 9, stddev_time: 0, context: map[string]uint64{}}
		m := mergeEvent(a, b)
		approx(t, "mean_time", m.mean_time, 7)
		approx(t, "stddev_time", m.stddev_time, 2)
	})

	t.Run("unequal counts", func(t *testing.T) {
		a := QueryEvent{calls: 3, mean_time: 10, stddev_time: 0, context: map[string]uint64{}}
		b := QueryEvent{calls: 1, mean_time: 22, stddev_time: 0, context: map[string]uint64{}}
		m := mergeEvent(a, b)
		approx(t, "mean_time", m.mean_time, 13)
		approx(t, "stddev_time", m.stddev_time, math.Sqrt(27))
	})

	t.Run("one-call side with zero stddev", func(t *testing.T) {
		a := QueryEvent{calls: 4, mean_time: 10, stddev_time: 2, context: map[string]uint64{}}
		b := QueryEvent{calls: 1, mean_time: 20, stddev_time: 0, context: map[string]uint64{}}
		m := mergeEvent(a, b)
		approx(t, "mean_time", m.mean_time, 12)
		approx(t, "stddev_time", m.stddev_time, math.Sqrt(19.2))
	})

	t.Run("nonzero stddevs match population samples", func(t *testing.T) {
		aSamples := []float64{8, 10, 12, 14}
		bSamples := []float64{12, 18, 24}
		a := queryEventFromSamples(aSamples)
		b := queryEventFromSamples(bSamples)
		m := mergeEvent(a, b)
		mean, sd := populationStats(append(aSamples, bSamples...))
		approx(t, "mean_time", m.mean_time, mean)
		approx(t, "stddev_time", m.stddev_time, sd)
	})

	t.Run("fractional calls use original counts", func(t *testing.T) {
		a := QueryEvent{calls: 1.5, mean_time: 4, stddev_time: 0, context: map[string]uint64{}}
		b := QueryEvent{calls: 1.9, mean_time: 10, stddev_time: 0, context: map[string]uint64{}}
		m := mergeEvent(a, b)
		approx(t, "calls", m.calls, 3.4)
		approx(t, "mean_time", m.mean_time, 25.0/3.4)
		approx(t, "stddev_time", m.stddev_time, math.Sqrt((36*(1.5*1.9/3.4))/3.4))
	})
}

func queryEventFromSamples(samples []float64) QueryEvent {
	mean, sd := populationStats(samples)
	return QueryEvent{calls: float64(len(samples)), mean_time: mean, stddev_time: sd, context: map[string]uint64{}}
}

func populationStats(samples []float64) (float64, float64) {
	var sum float64
	for _, sample := range samples {
		sum += sample
	}
	mean := sum / float64(len(samples))
	var m2 float64
	for _, sample := range samples {
		delta := sample - mean
		m2 += delta * delta
	}
	return mean, math.Sqrt(m2 / float64(len(samples)))
}

// TestMergeEventAbsentStddev: an absent stddev on either side makes the
// merged one absent (and 0), without touching the mean. Lifetime min and max
// on either side make the merged ones lifetime.
func TestMergeEventAbsentStddev(t *testing.T) {
	for _, c := range []struct{ a, b, want bool }{{false, false, false}, {true, false, true}, {false, true, true}, {true, true, true}} {
		a := QueryEvent{calls: 2, mean_time: 10, stddev_time: 2, stddev_absent: c.a, minmax_lifetime: c.a, context: map[string]uint64{}}
		b := QueryEvent{calls: 2, mean_time: 20, stddev_time: 4, stddev_absent: c.b, minmax_lifetime: c.b, context: map[string]uint64{}}
		m := mergeEvent(a, b)
		if m.stddev_absent != c.want || m.minmax_lifetime != c.want {
			t.Errorf("%v+%v: stddev_absent %v, minmax_lifetime %v, want %v", c.a, c.b, m.stddev_absent, m.minmax_lifetime, c.want)
		}
		if c.want && m.stddev_time != 0 {
			t.Errorf("%v+%v: absent stddev_time = %v, want 0", c.a, c.b, m.stddev_time)
		}
		if !c.want && m.stddev_time == 0 {
			t.Errorf("%v+%v: stddev_time = 0, want the merged value", c.a, c.b)
		}
		// Same mean as TestMergeEventRunningStat's first case.
		approx(t, "mean_time", m.mean_time, 15)
	}
}

func TestMergeEventContextHistogram(t *testing.T) {
	ctx := map[string]uint64{"": 3, "controller:1": 5}
	a := QueryEvent{calls: 8, context: ctx}

	m := mergeEvent(a, QueryEvent{calls: 2, context: map[string]uint64{"controller:1": 2}})
	if got := m.context["controller:1"]; got != 7 {
		t.Errorf("existing hash count = %d, want 7", got)
	}
	m = mergeEvent(m, QueryEvent{calls: 4, context: map[string]uint64{"controller:2action:3": 4}})
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

func TestMergeEventContextHistogramDoesNotWrapUint32(t *testing.T) {
	const want = uint64(1) << 32
	m := mergeEvent(
		QueryEvent{calls: float64(want / 2), context: map[string]uint64{"controller:big": want / 2}},
		QueryEvent{calls: float64(want / 2), context: map[string]uint64{"controller:big": want / 2}},
	)
	if got := uint64(m.context["controller:big"]); got != want {
		t.Fatalf("merged context count = %d, want %d", got, want)
	}
}

// Each side's histogram is built from integral Postgres call counters before
// merging. Fractional test-only values are rounded, not silently truncated.
func TestMergeEventContextRoundsFractionalCalls(t *testing.T) {
	b := QueryEvent{calls: 3.9}
	b.context = map[string]uint64{"job_tag:7": wholeCount(b.calls)}
	m := mergeEvent(QueryEvent{calls: 1, context: map[string]uint64{"job_tag:7": 1}}, b)
	if got := m.context["job_tag:7"]; got != 5 {
		t.Errorf("merged count = %d, want 5", got)
	}
	approx(t, "calls", m.calls, 4.9)
}

func TestWholeCountCapsAtContextLimit(t *testing.T) {
	if got := wholeCount(float64(harvestlimits.MaxContextCount) * 2); got != harvestlimits.MaxContextCount {
		t.Fatalf("wholeCount above limit = %d, want %d", got, harvestlimits.MaxContextCount)
	}
}
