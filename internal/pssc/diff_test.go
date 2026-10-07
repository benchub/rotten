package pssc

import (
	"sort"
	"testing"
	"time"
)

var (
	t0 = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	t1 = t0.Add(time.Hour)
)

func st(q int64, controller string, calls int64) Stat {
	return Stat{
		UserID: 10, DBID: 5, QueryID: q, TopLevel: true,
		Tags:  map[string]string{"controller": controller, "action": "a"},
		Calls: calls, ExecTime: 2 * float64(calls), StatsSince: t0,
	}
}

func snap(stats ...Stat) Snapshot {
	s := Snapshot{Entries: map[Key]Stat{}}
	for _, x := range stats {
		s.Entries[KeyOf(x)] = x
	}
	return s
}

type want struct {
	q          int64
	controller string
	calls      int64
	time       float64
	new        bool
}

func got(ds []Delta) []want {
	var w []want
	for _, d := range ds {
		w = append(w, want{d.QueryID, d.Tags["controller"], d.Calls, d.ExecTime, d.New})
	}
	sort.Slice(w, func(i, j int) bool {
		if w[i].q != w[j].q {
			return w[i].q < w[j].q
		}
		return w[i].controller < w[j].controller
	})
	return w
}

func TestDiff(t *testing.T) {
	reset := st(1, "c", 3)
	reset.StatsSince = t1
	evicted := st(1, "c", 40) // recreated and grew past the old counters
	evicted.StatsSince = t1
	lowerTime := st(1, "c", 12)
	lowerTime.ExecTime = 1 // below the snapshot's 20
	cases := []struct {
		name     string
		prev     Snapshot
		cur      []Stat
		want     []want
		wantSnap map[string]int64 // controller -> calls kept, for queryid 1
	}{
		{"growth", snap(st(1, "c", 10)), []Stat{st(1, "c", 15)},
			[]want{{1, "c", 5, 10, false}}, map[string]int64{"c": 15}},
		{"reset", snap(st(1, "c", 10)), []Stat{reset},
			[]want{{1, "c", 3, 6, true}}, map[string]int64{"c": 3}},
		{"eviction new stats_since", snap(st(1, "c", 10)), []Stat{evicted},
			[]want{{1, "c", 40, 80, true}}, map[string]int64{"c": 40}},
		{"calls lower", snap(st(1, "c", 10)), []Stat{st(1, "c", 4)},
			[]want{{1, "c", 4, 8, true}}, map[string]int64{"c": 4}},
		{"time lower", snap(st(1, "c", 10)), []Stat{lowerTime},
			[]want{{1, "c", 12, 1, true}}, map[string]int64{"c": 12}},
		{"missing from snapshot", snap(), []Stat{st(1, "c", 7)},
			[]want{{1, "c", 7, 14, true}}, map[string]int64{"c": 7}},
		{"zero calls omitted but kept", snap(st(1, "c", 10)), []Stat{st(1, "c", 10)},
			nil, map[string]int64{"c": 10}},
		{"tag sets are separate keys", snap(st(1, "c", 10), st(1, "d", 1)), []Stat{st(1, "c", 11), st(1, "d", 4)},
			[]want{{1, "c", 1, 2, false}, {1, "d", 3, 6, false}}, map[string]int64{"c": 11, "d": 4}},
		{"evicted entry dropped", snap(st(1, "c", 10), st(1, "d", 1)), []Stat{st(1, "c", 10)},
			nil, map[string]int64{"c": 10}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			before := map[Key]Stat{}
			for k, v := range c.prev.Entries {
				before[k] = v
			}
			ds, next := Diff(c.prev, c.cur)
			g := got(ds)
			if len(g) != len(c.want) {
				t.Fatalf("deltas = %+v, want %+v", g, c.want)
			}
			for i := range g {
				if g[i] != c.want[i] {
					t.Errorf("delta %d = %+v, want %+v", i, g[i], c.want[i])
				}
			}
			for k, v := range before {
				if c.prev.Entries[k].Calls != v.Calls || c.prev.Entries[k].ExecTime != v.ExecTime {
					t.Errorf("Diff modified prev entry %v", k)
				}
			}
			if len(next.Entries) != len(c.wantSnap) {
				t.Fatalf("next has %d entries, want %d", len(next.Entries), len(c.wantSnap))
			}
			for k, e := range next.Entries {
				if c.wantSnap[e.Tags["controller"]] != e.Calls || k != KeyOf(e) {
					t.Errorf("next entry %+v, want calls %d", e, c.wantSnap[e.Tags["controller"]])
				}
			}
		})
	}
}

func TestDiffDeltaHasPrev(t *testing.T) {
	ds, _ := Diff(snap(st(1, "c", 10)), []Stat{st(1, "c", 15)})
	if len(ds) != 1 || ds[0].Prev == nil || ds[0].Prev.Calls != 10 {
		t.Fatalf("deltas = %+v, want Prev with 10 calls", ds)
	}
	ds, _ = Diff(snap(), []Stat{st(1, "c", 15)})
	if len(ds) != 1 || ds[0].Prev != nil {
		t.Fatalf("new delta has Prev: %+v", ds)
	}
}

func TestKeyIsCanonicalAndDistinguishesCapped(t *testing.T) {
	a := Stat{QueryID: 1, Tags: map[string]string{"a": "1", "b": "2"}}
	b := Stat{QueryID: 1, Tags: map[string]string{"b": "2", "a": "1"}}
	if KeyOf(a) != KeyOf(b) {
		t.Fatalf("same tags, different keys: %v %v", KeyOf(a), KeyOf(b))
	}
	capped := Stat{QueryID: 1, Tags: map[string]string{"a": Capped, "b": "2"}}
	if KeyOf(a) == KeyOf(capped) {
		t.Fatal("capped value collides with a real one")
	}
	empty := Stat{QueryID: 1, Tags: map[string]string{"a": "", "b": "2"}}
	if KeyOf(empty) == KeyOf(capped) {
		t.Fatal("capped value collides with an empty string")
	}
	none := Stat{QueryID: 1}
	if KeyOf(none) != KeyOf(Stat{QueryID: 1, Tags: map[string]string{}}) {
		t.Fatal("nil and empty tags differ")
	}
	for _, s := range []Stat{a, capped, empty, none} {
		tags, err := ParseTags(KeyOf(s).Tags)
		if err != nil || len(tags) != len(s.Tags) {
			t.Fatalf("ParseTags(%q) = %v, %v", KeyOf(s).Tags, tags, err)
		}
		for k, v := range s.Tags {
			if tags[k] != v {
				t.Errorf("ParseTags(%q)[%q] = %q, want %q", KeyOf(s).Tags, k, tags[k], v)
			}
		}
	}
}
