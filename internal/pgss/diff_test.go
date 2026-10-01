package pgss

import (
	"sort"
	"testing"
	"time"
)

func ptrT(t time.Time) *time.Time { return &t }
func ptrF(f float64) *float64     { return &f }
func ptrI(i int64) *int64         { return &i }

var (
	t0 = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	t1 = t0.Add(time.Hour)
)

// st builds a Stat with every cumulative counter derived from calls, so a
// later stat with more calls is higher on every counter.
func st(q int64, calls int64) Stat {
	f := float64(calls)
	return Stat{
		UserID: 10, DBID: 5, TopLevel: true, QueryID: q,
		Plans: calls, TotalPlanTime: f, Calls: calls, TotalExecTime: 2 * f, TotalTime: 3 * f,
		MinTime: 1, MaxTime: 9, MeanTime: 2, StddevTime: 0.5,
		Rows: calls, SharedBlksHit: calls, SharedBlksRead: calls, SharedBlksDirtied: calls,
		SharedBlksWritten: calls, LocalBlksHit: calls, LocalBlksRead: calls,
		LocalBlksDirtied: calls, LocalBlksWritten: calls, TempBlksRead: calls,
		TempBlksWritten: calls, SharedBlkReadTime: f, SharedBlkWriteTime: f,
		WALRecords: calls, WALFPI: calls, WALBytes: f,
		TempBlkReadTime: ptrF(f), TempBlkWriteTime: ptrF(f),
		LocalBlkReadTime: ptrF(f), LocalBlkWriteTime: ptrF(f),
		StatsSince: ptrT(t0), MinmaxStatsSince: ptrT(t0),
		WALBuffersFull: ptrI(calls), ParallelWorkersToLaunch: ptrI(calls),
		ParallelWorkersLaunched: ptrI(calls),
	}
}

func snap(info Info, stats ...Stat) Snapshot {
	s := Snapshot{Info: info, Entries: map[Key]Stat{}}
	for _, x := range stats {
		s.Entries[KeyOf(x)] = x
	}
	return s
}

type want struct {
	q     int64
	calls int64
	new   bool
}

func TestDiff(t *testing.T) {
	info0 := Info{StatsReset: t0}
	info1 := Info{StatsReset: t1}
	lowerRows := st(1, 12)
	lowerRows.Rows = 3 // below the snapshot's 10
	lowerPtr := st(1, 12)
	lowerPtr.WALBuffersFull = ptrI(1)
	entryReset := st(1, 4)
	entryReset.StatsSince = ptrT(t1)

	cases := []struct {
		name     string
		prev     Snapshot
		cur      []Stat
		info     Info
		want     []want
		wantSnap map[int64]int64 // queryid -> calls kept in next snapshot (key userid 10, top level)
	}{
		{"global reset", snap(info0, st(1, 10), st(2, 10)), []Stat{st(1, 15), st(2, 3)}, info1,
			[]want{{1, 15, true}, {2, 3, true}}, map[int64]int64{1: 15, 2: 3}},
		{"entry reset", snap(info0, st(1, 10)), []Stat{entryReset}, info0,
			[]want{{1, 4, true}}, map[int64]int64{1: 4}},
		{"counter lower", snap(info0, st(1, 10)), []Stat{lowerRows}, info0,
			[]want{{1, 12, true}}, map[int64]int64{1: 12}},
		{"optional counter lower", snap(info0, st(1, 10)), []Stat{lowerPtr}, info0,
			[]want{{1, 12, true}}, map[int64]int64{1: 12}},
		{"entry reset with higher counters", snap(info0, st(1, 10)), []Stat{entryResetHigh()}, info0,
			[]want{{1, 14, true}}, map[int64]int64{1: 14}},
		{"optional counter appeared", snap(info0, nilWAL(st(1, 10))), []Stat{st(1, 14)}, info0,
			[]want{{1, 14, true}}, map[int64]int64{1: 14}},
		{"optional counter vanished", snap(info0, st(1, 10)), []Stat{nilWAL(st(1, 14))}, info0,
			[]want{{1, 14, true}}, map[int64]int64{1: 14}},
		{"not in snapshot", snap(info0), []Stat{st(1, 6)}, info0,
			[]want{{1, 6, true}}, map[int64]int64{1: 6}},
		{"subtract", snap(info0, st(1, 10)), []Stat{st(1, 14)}, info0,
			[]want{{1, 4, false}}, map[int64]int64{1: 14}},
		{"evicted", snap(info0, st(1, 10), st(2, 10)), []Stat{st(1, 14)}, info0,
			[]want{{1, 4, false}}, map[int64]int64{1: 14}},
		{"zero calls kept in snapshot", snap(info0, st(1, 10)), []Stat{st(1, 10)}, info0,
			nil, map[int64]int64{1: 10}},
		{"mix", snap(info0, st(1, 10), st(2, 10), st(3, 10), st(4, 10), st(5, 10)),
			[]Stat{st(1, 14), lowerRowsQ(3), st(4, 10), st(6, 2), entryResetQ(5)}, info0,
			[]want{{1, 4, false}, {3, 12, true}, {5, 4, true}, {6, 2, true}},
			map[int64]int64{1: 14, 3: 12, 4: 10, 5: 4, 6: 2}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, next := Diff(c.prev, c.cur, c.info)
			sort.Slice(got, func(i, j int) bool { return got[i].QueryID < got[j].QueryID })
			if len(got) != len(c.want) {
				t.Fatalf("got %d deltas, want %d: %+v", len(got), len(c.want), got)
			}
			for i, w := range c.want {
				g := got[i]
				if g.QueryID != w.q || g.Calls != w.calls || g.New != w.new {
					t.Errorf("delta %d: got q=%d calls=%d new=%v, want %+v", i, g.QueryID, g.Calls, g.New, w)
				}
			}
			if next.Info != c.info {
				t.Errorf("next.Info = %+v, want %+v", next.Info, c.info)
			}
			if len(next.Entries) != len(c.wantSnap) {
				t.Fatalf("next snapshot has %d entries, want %d", len(next.Entries), len(c.wantSnap))
			}
			for q, calls := range c.wantSnap {
				e, ok := next.Entries[Key{UserID: 10, DBID: 5, TopLevel: true, QueryID: q}]
				if !ok || e.Calls != calls {
					t.Errorf("snapshot q=%d: got %+v ok=%v, want calls %d", q, e.Calls, ok, calls)
				}
			}
		})
	}
}

func entryResetHigh() Stat { s := st(1, 14); s.StatsSince = ptrT(t1); return s }
func nilWAL(s Stat) Stat   { s.WALBuffersFull = nil; return s }

// TestDiffPrev checks that a subtracted delta carries the snapshot entry and
// a new one doesn't.
func TestDiffPrev(t *testing.T) {
	old := st(1, 10)
	old.MeanTime, old.StddevTime = 7, 3
	got, _ := Diff(snap(Info{StatsReset: t0}, old, st(2, 10)),
		[]Stat{st(1, 14), entryResetQ(2), st(3, 1)}, Info{StatsReset: t0})
	sort.Slice(got, func(i, j int) bool { return got[i].QueryID < got[j].QueryID })
	if len(got) != 3 {
		t.Fatalf("got %d deltas", len(got))
	}
	p := got[0].Prev
	if p == nil || p.Calls != 10 || p.MeanTime != 7 || p.StddevTime != 3 {
		t.Errorf("Prev = %+v, want the snapshot entry", p)
	}
	*p.WALBuffersFull = 999
	*p.StatsSince = t1
	if *old.WALBuffersFull != 10 || !old.StatsSince.Equal(t0) {
		t.Errorf("writing through Prev changed the prev snapshot")
	}
	if got[1].Prev != nil || got[2].Prev != nil {
		t.Errorf("new deltas carry Prev: %+v, %+v", got[1].Prev, got[2].Prev)
	}
}

func lowerRowsQ(q int64) Stat { s := st(q, 12); s.Rows = 3; return s }
func entryResetQ(q int64) Stat {
	s := st(q, 4)
	s.StatsSince = ptrT(t1)
	return s
}

// TestDiffSubtractsEveryCounter checks each cumulative counter is subtracted
// and each non-cumulative field is passed through from cur.
func TestDiffSubtractsEveryCounter(t *testing.T) {
	prev := snap(Info{StatsReset: t0}, st(1, 10))
	cur := st(1, 14)
	cur.MinTime, cur.MaxTime, cur.MeanTime, cur.StddevTime = 0.1, 99, 3, 4
	cur.MinmaxStatsSince = ptrT(t1)
	got, _ := Diff(prev, []Stat{cur}, Info{StatsReset: t0})
	if len(got) != 1 {
		t.Fatalf("got %d deltas", len(got))
	}
	want := st(1, 4)
	want.MinTime, want.MaxTime, want.MeanTime, want.StddevTime = 0.1, 99, 3, 4
	want.MinmaxStatsSince = ptrT(t1)
	d := got[0].Stat
	if d.Plans != 4 || d.TotalPlanTime != 4 || d.TotalExecTime != 8 || d.TotalTime != 12 ||
		d.Rows != 4 || d.SharedBlksHit != 4 || d.SharedBlksRead != 4 || d.SharedBlksDirtied != 4 ||
		d.SharedBlksWritten != 4 || d.LocalBlksHit != 4 || d.LocalBlksRead != 4 ||
		d.LocalBlksDirtied != 4 || d.LocalBlksWritten != 4 || d.TempBlksRead != 4 ||
		d.TempBlksWritten != 4 || d.SharedBlkReadTime != 4 || d.SharedBlkWriteTime != 4 ||
		d.WALRecords != 4 || d.WALFPI != 4 || d.WALBytes != 4 ||
		*d.TempBlkReadTime != 4 || *d.TempBlkWriteTime != 4 || *d.LocalBlkReadTime != 4 ||
		*d.LocalBlkWriteTime != 4 || *d.WALBuffersFull != 4 || *d.ParallelWorkersToLaunch != 4 ||
		*d.ParallelWorkersLaunched != 4 {
		t.Errorf("counters not subtracted: %+v", d)
	}
	if d.MinTime != 0.1 || d.MaxTime != 99 || d.MeanTime != 3 || d.StddevTime != 4 ||
		!d.MinmaxStatsSince.Equal(t1) || !d.StatsSince.Equal(t0) {
		t.Errorf("non-cumulative fields not passed through: %+v", d)
	}
	// Diff must not alias the snapshot's pointers.
	if *prev.Entries[KeyOf(cur)].WALBuffersFull != 10 {
		t.Errorf("Diff mutated prev snapshot")
	}
}

// TestDiffKey checks that userid and toplevel are part of the key.
func TestDiffKey(t *testing.T) {
	prev := snap(Info{StatsReset: t0}, st(1, 10))
	cur := []Stat{st(1, 14), func() Stat { s := st(1, 7); s.UserID = 11; return s }(),
		func() Stat { s := st(1, 7); s.TopLevel = false; return s }(),
		func() Stat { s := st(1, 7); s.DBID = 6; return s }()}
	got, next := Diff(prev, cur, Info{StatsReset: t0})
	if len(got) != 4 || len(next.Entries) != 4 {
		t.Fatalf("got %d deltas and %d entries, want 4 and 4", len(got), len(next.Entries))
	}
	news := 0
	for _, d := range got {
		if d.New {
			news++
		}
	}
	if news != 3 {
		t.Errorf("got %d new, want 3", news)
	}
}
