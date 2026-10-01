package pgss

import "time"

// Key identifies one pg_stat_statements entry.
type Key struct {
	UserID   uint32
	DBID     uint32
	TopLevel bool
	QueryID  int64
}

// KeyOf returns the snapshot key of s.
func KeyOf(s Stat) Key {
	return Key{UserID: s.UserID, DBID: s.DBID, TopLevel: s.TopLevel, QueryID: s.QueryID}
}

// Snapshot is the cumulative state from the last harvest: the info row and
// every entry as read, keyed by Key.
type Snapshot struct {
	Info    Info
	Entries map[Key]Stat
}

// Delta is one entry's activity for a window.
//
// Cumulative counters (see counters) hold the window's change. Everything
// else (Query, MinTime, MaxTime, MeanTime, StddevTime, StatsSince,
// MinmaxStatsSince) is the current value, passed through unchanged; mean and
// stddev for the window are worked out from deltas elsewhere (-20), and min
// and max are handled by -21.
//
// New is true when the entry was treated as new (global reset, entry reset, a
// counter went down, or no snapshot entry), so the counters are the entry's
// full current values rather than a difference.
//
// Prev is the snapshot entry the counters were subtracted from, so callers
// can rebuild the old side's sums (for example stddev from the old mean,
// stddev, and calls). It's nil when New is true.
type Delta struct {
	Stat
	New  bool
	Prev *Stat
}

// counter visits every cumulative counter of a, b (b from the snapshot).
// Optional (pointer) counters are visited only when both sides have them.
func counters(a, b *Stat, i func(x, y *int64), f func(x, y *float64)) {
	i(&a.Plans, &b.Plans)
	f(&a.TotalPlanTime, &b.TotalPlanTime)
	i(&a.Calls, &b.Calls)
	f(&a.TotalExecTime, &b.TotalExecTime)
	f(&a.TotalTime, &b.TotalTime)
	i(&a.Rows, &b.Rows)
	i(&a.SharedBlksHit, &b.SharedBlksHit)
	i(&a.SharedBlksRead, &b.SharedBlksRead)
	i(&a.SharedBlksDirtied, &b.SharedBlksDirtied)
	i(&a.SharedBlksWritten, &b.SharedBlksWritten)
	i(&a.LocalBlksHit, &b.LocalBlksHit)
	i(&a.LocalBlksRead, &b.LocalBlksRead)
	i(&a.LocalBlksDirtied, &b.LocalBlksDirtied)
	i(&a.LocalBlksWritten, &b.LocalBlksWritten)
	i(&a.TempBlksRead, &b.TempBlksRead)
	i(&a.TempBlksWritten, &b.TempBlksWritten)
	f(&a.SharedBlkReadTime, &b.SharedBlkReadTime)
	f(&a.SharedBlkWriteTime, &b.SharedBlkWriteTime)
	i(&a.WALRecords, &b.WALRecords)
	i(&a.WALFPI, &b.WALFPI)
	f(&a.WALBytes, &b.WALBytes)
	for _, p := range [][2]*float64{
		{a.TempBlkReadTime, b.TempBlkReadTime}, {a.TempBlkWriteTime, b.TempBlkWriteTime},
		{a.LocalBlkReadTime, b.LocalBlkReadTime}, {a.LocalBlkWriteTime, b.LocalBlkWriteTime},
	} {
		if p[0] != nil && p[1] != nil {
			f(p[0], p[1])
		}
	}
	for _, p := range [][2]*int64{
		{a.WALBuffersFull, b.WALBuffersFull},
		{a.ParallelWorkersToLaunch, b.ParallelWorkersToLaunch},
		{a.ParallelWorkersLaunched, b.ParallelWorkersLaunched},
	} {
		if p[0] != nil && p[1] != nil {
			i(p[0], p[1])
		}
	}
}

// optMismatch reports whether an optional counter is present on one side
// only, which means the extension changed under us.
func optMismatch(a, b *Stat) bool {
	f := func(x, y *float64) bool { return (x == nil) != (y == nil) }
	i := func(x, y *int64) bool { return (x == nil) != (y == nil) }
	return f(a.TempBlkReadTime, b.TempBlkReadTime) || f(a.TempBlkWriteTime, b.TempBlkWriteTime) ||
		f(a.LocalBlkReadTime, b.LocalBlkReadTime) || f(a.LocalBlkWriteTime, b.LocalBlkWriteTime) ||
		i(a.WALBuffersFull, b.WALBuffersFull) ||
		i(a.ParallelWorkersToLaunch, b.ParallelWorkersToLaunch) ||
		i(a.ParallelWorkersLaunched, b.ParallelWorkersLaunched)
}

func anyLower(cur, old Stat) bool {
	lower := false
	counters(&cur, &old,
		func(x, y *int64) { lower = lower || *x < *y },
		func(x, y *float64) { lower = lower || *x < *y })
	return lower
}

// clonePtrs gives s its own copies of its optional counters, so subtracting
// in place doesn't write through to cur or the snapshot.
func clonePtrs(s *Stat) {
	cf := func(p **float64) {
		if *p != nil {
			v := **p
			*p = &v
		}
	}
	ci := func(p **int64) {
		if *p != nil {
			v := **p
			*p = &v
		}
	}
	ct := func(p **time.Time) {
		if *p != nil {
			v := **p
			*p = &v
		}
	}
	ct(&s.StatsSince)
	ct(&s.MinmaxStatsSince)
	cf(&s.TempBlkReadTime)
	cf(&s.TempBlkWriteTime)
	cf(&s.LocalBlkReadTime)
	cf(&s.LocalBlkWriteTime)
	ci(&s.WALBuffersFull)
	ci(&s.ParallelWorkersToLaunch)
	ci(&s.ParallelWorkersLaunched)
}

func sameTime(a, b *Stat) bool {
	x, y := a.StatsSince, b.StatsSince
	if x == nil || y == nil {
		return x == nil && y == nil
	}
	return x.Equal(*y)
}

// Diff compares the current harvest against the previous snapshot and
// returns each entry's activity for the window plus the snapshot to keep for
// next time. It's pure: no database and no I/O, and it doesn't modify prev or
// cur. Rules, checked in order for each entry:
//
//  1. info.StatsReset differs from prev.Info.StatsReset: new.
//  2. StatsSince changed (17+): new.
//  3. Any cumulative counter is lower than the snapshot, or an optional
//     counter is present on only one side: new.
//  4. Not in the snapshot: new.
//  5. Otherwise, current minus snapshot.
//
// Entries missing from cur (evicted) are dropped from next. Every entry in
// cur goes into next as read. Deltas with zero calls are left out of the
// result but still kept in next. That also drops planning-only activity
// (Plans or TotalPlanTime grew while Calls didn't). This is deliberate.
//
// On 14 through 16 there's no stats_since, so rule 2 can't fire. An entry
// that's reset on its own and then grows past its old counters before the
// next harvest can't be detected there, and its delta comes out too small.
//
// Diff doesn't handle the first-run baseline: an empty prev with a zero
// StatsReset trips rule 1 and returns everything as new. The caller decides
// whether a snapshot counts as a baseline.
func Diff(prev Snapshot, cur []Stat, info Info) ([]Delta, Snapshot) {
	next := Snapshot{Info: info, Entries: make(map[Key]Stat, len(cur))}
	globalReset := !prev.Info.StatsReset.Equal(info.StatsReset)
	var deltas []Delta
	for _, c := range cur {
		k := KeyOf(c)
		next.Entries[k] = c
		old, ok := prev.Entries[k]
		d := Delta{Stat: c}
		switch {
		case globalReset, ok && !sameTime(&c, &old), ok && (optMismatch(&c, &old) || anyLower(c, old)), !ok:
			d.New = true
		default:
			o := old
			clonePtrs(&o)
			d.Prev = &o
			clonePtrs(&d.Stat)
			counters(&d.Stat, &old,
				func(x, y *int64) { *x -= *y },
				func(x, y *float64) { *x -= *y })
		}
		if d.Calls == 0 {
			continue
		}
		deltas = append(deltas, d)
	}
	return deltas, next
}
