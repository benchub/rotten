package pgss

import (
	"sort"
	"testing"
)

func pre17(s Stat) Stat { s.StatsSince, s.MinmaxStatsSince = nil, nil; return s }

func TestRecreated(t *testing.T) {
	info0 := Info{StatsReset: t0}
	info1 := Info{StatsReset: t1}
	dealloc := Info{StatsReset: t0, Dealloc: 3}
	cases := []struct {
		name string
		prev Snapshot
		cur  []Stat
		info Info
		want []int64
	}{
		{"unchanged", snap(info0, st(1, 10), st(2, 10)), []Stat{st(1, 14), st(2, 10)}, info0, nil},
		{"global reset", snap(info0, st(1, 10), st(2, 10)), []Stat{st(1, 15), st(2, 3)}, info1, []int64{1, 2}},
		{"empty baseline", Snapshot{}, []Stat{st(1, 15)}, info0, []int64{1}},
		{"stats_since changed, counters higher", snap(info0, st(1, 10), st(2, 10)), []Stat{entryResetHigh(), st(2, 12)}, info0, []int64{1}},
		{"counter lower", snap(info0, st(1, 10), st(2, 10)), []Stat{lowerRowsQ(1), st(2, 12)}, info0, []int64{1}},
		{"optional counter vanished", snap(info0, st(1, 10)), []Stat{nilWAL(st(1, 14))}, info0, []int64{1}},
		{"not in snapshot", snap(info0, st(1, 10)), []Stat{st(1, 14), st(2, 1)}, info0, []int64{2}},
		{"dealloc before 17, counters higher", snap(info0, pre17(st(1, 10)), pre17(st(2, 10))),
			[]Stat{pre17(st(1, 14)), pre17(st(2, 10))}, dealloc, []int64{1, 2}},
		{"no dealloc before 17", snap(dealloc, pre17(st(1, 10))), []Stat{pre17(st(1, 14))}, dealloc, nil},
		{"dealloc on 17+, same stats_since", snap(info0, st(1, 10), st(2, 10)), []Stat{st(1, 14), st(2, 10)}, dealloc, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got []int64
			for _, k := range Recreated(c.prev, c.cur, c.info) {
				got = append(got, k.QueryID)
			}
			sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
			if len(got) != len(c.want) {
				t.Fatalf("Recreated = %v, want %v", got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("Recreated = %v, want %v", got, c.want)
				}
			}
		})
	}
}

func TestTextCacheInvalidate(t *testing.T) {
	c := NewTextCache(nil)
	a, b := KeyOf(st(1, 1)), KeyOf(st(2, 1))
	c.text[a], c.text[b] = "old a", "b"
	c.Invalidate([]Key{a})
	if c.Has(a) || !c.Has(b) {
		t.Fatalf("after Invalidate(a): Has(a)=%v Has(b)=%v, want false true", c.Has(a), c.Has(b))
	}
}
