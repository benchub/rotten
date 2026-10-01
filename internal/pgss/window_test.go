package pgss

import (
	"math"
	"math/rand"
	"testing"
)

// cumulative mimics pg_stat_statements: total, Welford mean, and population
// stddev (sqrt(sum_var / calls), 0 when calls <= 1).
func cumulative(xs []float64) Stat {
	var s Stat
	var mean, m2 float64
	for i, x := range xs {
		s.TotalExecTime += x
		old := mean
		mean += (x - old) / float64(i+1)
		m2 += (x - old) * (x - mean)
	}
	s.Calls = int64(len(xs))
	s.TotalTime = s.TotalExecTime
	s.MeanTime = mean
	if s.Calls > 1 {
		s.StddevTime = math.Sqrt(m2 / float64(s.Calls))
	}
	s.QueryID = 1
	return s
}

// direct computes the population mean and stddev of xs in two passes.
func direct(xs []float64) (mean, sd float64) {
	for _, x := range xs {
		mean += x
	}
	mean /= float64(len(xs))
	var v float64
	for _, x := range xs {
		v += (x - mean) * (x - mean)
	}
	if len(xs) > 1 {
		sd = math.Sqrt(v / float64(len(xs)))
	}
	return mean, sd
}

func samples(r *rand.Rand, n int, base, spread float64) []float64 {
	xs := make([]float64, n)
	for i := range xs {
		xs[i] = base + r.Float64()*spread
	}
	return xs
}

// windowOf diffs cumulative(a+b) against a snapshot of cumulative(a).
func windowOf(t *testing.T, a, b []float64) []Delta {
	t.Helper()
	pa := cumulative(a)
	cur := cumulative(append(append([]float64{}, a...), b...))
	ds, _ := Diff(Snapshot{Entries: map[Key]Stat{KeyOf(pa): pa}}, []Stat{cur}, Info{})
	return ds
}

func checkWindow(t *testing.T, a, b []float64) {
	t.Helper()
	ds := windowOf(t, a, b)
	if len(ds) != 1 || ds[0].New || ds[0].Prev == nil {
		t.Fatalf("want one diffed delta, got %+v", ds)
	}
	gm, gs, ok := WindowStats(ds[0])
	wm, ws := direct(b)
	if math.Abs(gm-wm) > 1e-9 || math.Abs(gs-ws) > 1e-9 || !ok {
		t.Errorf("len(a)=%d len(b)=%d: got mean %.15g sd %.15g, want %.15g %.15g",
			len(a), len(b), gm, gs, wm, ws)
	}
}

func TestWindowStatsMatchesDirect(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 200; i++ {
		a := samples(r, 1+r.Intn(500), r.Float64()*10, r.Float64()*100)
		b := samples(r, 1+r.Intn(500), r.Float64()*10, r.Float64()*100)
		checkWindow(t, a, b)
	}
}

func TestWindowStatsEdges(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	t.Run("prev calls 1", func(t *testing.T) { checkWindow(t, []float64{3.5}, samples(r, 50, 1, 9)) })
	t.Run("single call window", func(t *testing.T) { checkWindow(t, samples(r, 50, 1, 9), []float64{42}) })
	t.Run("both one call", func(t *testing.T) { checkWindow(t, []float64{1}, []float64{9}) })
	t.Run("constant times", func(t *testing.T) {
		ds := windowOf(t, []float64{0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1}, []float64{0.1, 0.1, 0.1})
		_, sd, ok := WindowStats(ds[0])
		if math.IsNaN(sd) || sd < 0 || sd > 1e-9 || !ok {
			t.Errorf("constant window stddev = %v, want ~0", sd)
		}
	})
	t.Run("zero calls window", func(t *testing.T) {
		if ds := windowOf(t, samples(r, 10, 1, 1), nil); len(ds) != 0 {
			t.Fatalf("Δcalls = 0 gave %d deltas", len(ds))
		}
		// Called directly anyway, it must not divide by zero.
		m, sd, ok := WindowStats(Delta{Prev: &Stat{Calls: 3}})
		if m != 0 || sd != 0 || !ok {
			t.Errorf("zero calls: got %v %v, want 0 0", m, sd)
		}
	})
}

func TestWindowStatsNew(t *testing.T) {
	s := cumulative([]float64{1, 2, 3, 10})
	m, sd, ok := WindowStats(Delta{Stat: s, New: true})
	if m != s.MeanTime || sd != s.StddevTime || !ok {
		t.Errorf("new: got %v %v, want %v %v", m, sd, s.MeanTime, s.StddevTime)
	}
}

// A small window after 1e9 calls: the stddev subtraction cancels, and a
// 1e-9 relative error in pgss's StddevTime must be flagged, not reported.
func TestWindowStatsCancellation(t *testing.T) {
	const np, nw = 1e9, 100
	m2p, m2w := 10.0*10*np, 5.0*5*nw // prev sd 10, window sd 5, same mean 10
	prev := Stat{Calls: np, TotalExecTime: 10 * np, StddevTime: 10}
	sdc := math.Sqrt((m2p + m2w) / (np + nw))
	for _, eps := range []float64{0, 1e-9, -1e-9} {
		d := Delta{Prev: &prev, Stat: Stat{Calls: nw, TotalExecTime: 10 * nw, StddevTime: sdc * (1 + eps)}}
		m, sd, ok := WindowStats(d)
		if m != 10 {
			t.Errorf("eps %g: mean %v, want 10", eps, m)
		}
		if ok || math.IsNaN(sd) {
			t.Errorf("eps %g: got sd %v ok %v, want ok false", eps, sd, ok)
		}
	}
}
