package worker

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/pgss"
	"github.com/benchub/rotten/internal/pssc"
)

func psscDelta(qid int64, calls int64, execTime float64, tags map[string]string) pssc.Delta {
	return pssc.Delta{Stat: pssc.Stat{UserID: 1, DBID: 2, QueryID: qid, TopLevel: true, Tags: tags,
		Calls: calls, ExecTime: execTime, StatsSince: time.Unix(100, 0)}}
}

func pgssDelta(qid int64, calls int64, execTime float64) pgss.Delta {
	return pgss.Delta{Stat: pgss.Stat{UserID: 1, DBID: 2, QueryID: qid, TopLevel: true, Calls: calls, TotalExecTime: execTime}}
}

func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	old := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(old) })
	return &buf
}

func TestForDeltaRealCountsAndTimesWithUntaggedRemainder(t *testing.T) {
	pc := newPSSCContexts(true, []pssc.Delta{
		psscDelta(7, 3, 30, map[string]string{"controller": "users", "action": "show"}),
		psscDelta(7, 5, 5, map[string]string{"controller": "posts", "action": "index", "job": "J"}),
		// Same three tags, another key: summed into one context.
		psscDelta(7, 1, 2, map[string]string{"controller": "users", "action": "show", "route": "/u"}),
		// None of the three: untagged.
		psscDelta(7, 1, 4, map[string]string{"route": "/x"}),
		// Another queryid, and the same queryid not top level: not ours.
		psscDelta(8, 50, 50, map[string]string{"controller": "other"}),
		{Stat: pssc.Stat{UserID: 1, DBID: 2, QueryID: 7, TopLevel: false, Tags: map[string]string{"controller": "nested"}, Calls: 9}},
	}, pssc.Snapshot{}, pssc.Snapshot{}, time.Unix(50, 0))
	counts, times := pc.forDelta(pgssDelta(7, 12, 100))
	wantCounts := map[string]uint64{
		contextKey("users", "show", ""):   4,
		contextKey("posts", "index", "J"): 5,
		untaggedContextKey:                3,
	}
	wantTimes := map[string]float64{
		contextKey("users", "show", ""):   32,
		contextKey("posts", "index", "J"): 5,
		untaggedContextKey:                63,
	}
	if fmt.Sprint(counts) != fmt.Sprint(wantCounts) || fmt.Sprint(times) != fmt.Sprint(wantTimes) {
		t.Fatalf("counts=%v times=%v, want %v %v", counts, times, wantCounts, wantTimes)
	}
}

func TestForDeltaJobTagKey(t *testing.T) {
	pc := newPSSCContexts(true, []pssc.Delta{
		psscDelta(7, 2, 1, map[string]string{"job_tag": "Cleanup"}),
		psscDelta(7, 1, 1, map[string]string{"job": "A", "job_tag": "B"}),
	}, pssc.Snapshot{}, pssc.Snapshot{}, time.Unix(50, 0))
	counts, _ := pc.forDelta(pgssDelta(7, 3, 2))
	if counts[contextKey("", "", "Cleanup")] != 2 || counts[contextKey("", "", "A")] != 1 || len(counts) != 2 {
		t.Fatalf("counts = %v, want job_tag Cleanup 2 and job A 1", counts)
	}
}

func TestForDeltaWithoutPSSCIsAllUntagged(t *testing.T) {
	counts, times := psscContexts{}.forDelta(pgssDelta(7, 6, 12))
	if len(counts) != 1 || counts[untaggedContextKey] != 6 || times[untaggedContextKey] != 12 {
		t.Fatalf("counts=%v times=%v, want untagged 6 calls, 12ms", counts, times)
	}
}

func TestForDeltaPSSCAbovePGSSClampsUntaggedAndLogs(t *testing.T) {
	logs := captureLog(t)
	pc := newPSSCContexts(true, []pssc.Delta{
		psscDelta(7, 6, 60, map[string]string{"controller": "a"}),
		psscDelta(7, 2, 20, map[string]string{"controller": "b"}),
	}, pssc.Snapshot{}, pssc.Snapshot{}, time.Unix(50, 0))
	counts, times := pc.forDelta(pgssDelta(7, 4, 40))
	var sum uint64
	for _, c := range counts {
		sum += c
	}
	if sum > 4 {
		t.Fatalf("counts %v sum to %d, over pgss's 4", counts, sum)
	}
	if counts[contextKey("a", "", "")] != 3 || counts[contextKey("b", "", "")] != 1 {
		t.Fatalf("counts = %v, want a:3 b:1 (scaled by 4/8)", counts)
	}
	if _, ok := counts[untaggedContextKey]; ok {
		t.Fatalf("counts = %v, want no untagged context", counts)
	}
	if times[contextKey("a", "", "")] != 30 {
		t.Fatalf("times = %v, want a scaled to 30", times)
	}
	var held int64
	for _, h := range pc.held {
		held += h.calls
	}
	if held != 4 {
		t.Fatalf("held = %+v, want 4 calls held back", pc.held)
	}
	// Per-entry logging would flood on busy queries; the harvest logs one
	// summary instead (TestPSSCExcessIsHeldForTheNextWindow).
	if logs.Len() != 0 {
		t.Fatalf("forDelta logged: %s", logs.String())
	}
}

func TestForDeltaCappedTagShipsMarker(t *testing.T) {
	pc := newPSSCContexts(true, []pssc.Delta{
		psscDelta(7, 2, 1, map[string]string{"controller": pssc.Capped, "action": "show"}),
	}, pssc.Snapshot{}, pssc.Snapshot{}, time.Unix(50, 0))
	counts, _ := pc.forDelta(pgssDelta(7, 2, 1))
	if counts[contextKey(cappedTagValue, "show", "")] != 2 || len(counts) != 1 {
		t.Fatalf("counts = %v, want (capped)/show 2", counts)
	}
}

// A New entry that started before the previous harvest has lifetime values
// (the last harvest couldn't read pssc), so its calls stay untagged.
func TestNewPSSCEntryFromBeforeTheWindowIsUntagged(t *testing.T) {
	logs := captureLog(t)
	old := psscDelta(7, 100, 100, map[string]string{"controller": "old"})
	old.New = true
	fresh := psscDelta(7, 2, 2, map[string]string{"controller": "fresh"})
	fresh.New = true
	fresh.StatsSince = time.Unix(200, 0)
	pc := newPSSCContexts(true, []pssc.Delta{old, fresh}, pssc.Snapshot{}, pssc.Snapshot{}, time.Unix(150, 0))
	counts, _ := pc.forDelta(pgssDelta(7, 5, 5))
	if len(counts) != 2 || counts[contextKey("fresh", "", "")] != 2 || counts[untaggedContextKey] != 3 {
		t.Fatalf("counts = %v, want fresh 2 and untagged 3", counts)
	}
	if !strings.Contains(logs.String(), "started before this window") {
		t.Fatalf("no log for the skipped entry: %s", logs.String())
	}
}

// With a non-empty previous snapshot, pssc was read last harvest, so a New
// entry is genuinely new even if its stats_since looks a little older than
// the worker's taken_at (the observed server's clock running behind).
func TestNewPSSCEntryStaysTaggedWhenPreviousSnapshotIsNotEmpty(t *testing.T) {
	other := psscDelta(9, 1, 1, map[string]string{"controller": "x"}).Stat
	prev := pssc.Snapshot{Entries: map[pssc.Key]pssc.Stat{pssc.KeyOf(other): other}}
	fresh := psscDelta(7, 2, 2, map[string]string{"controller": "fresh"})
	fresh.New = true
	fresh.StatsSince = time.Unix(149, 0)
	pc := newPSSCContexts(true, []pssc.Delta{fresh}, prev, pssc.Snapshot{}, time.Unix(150, 0))
	counts, _ := pc.forDelta(pgssDelta(7, 2, 2))
	if len(counts) != 1 || counts[contextKey("fresh", "", "")] != 2 {
		t.Fatalf("counts = %v, want fresh 2", counts)
	}
}

// When a pgss entry's text fetch fails and its snapshot is carried, its
// pssc entries are carried too, so next window's two deltas cover the same
// span. Other keys keep the fresh read.
func TestTextSkipCarriesMatchingPSSCEntries(t *testing.T) {
	w := New(Config{LogicalID: 7, PhysicalID: 42}, RealClock{})
	key := pgss.Key{UserID: 1, DBID: 2, TopLevel: true, QueryID: 7}
	oldStat := pgss.Stat{UserID: 1, DBID: 2, TopLevel: true, QueryID: 7, Calls: 10, TotalExecTime: 10}
	curStat := oldStat
	curStat.Calls, curStat.TotalExecTime = 13, 13
	prev := pgss.Snapshot{Entries: map[pgss.Key]pgss.Stat{key: oldStat}}
	next := pgss.Snapshot{Entries: map[pgss.Key]pgss.Stat{key: curStat}}
	deltas := []pgss.Delta{{Stat: pgss.Stat{UserID: 1, DBID: 2, TopLevel: true, QueryID: 7, Calls: 3, TotalExecTime: 3}, Prev: &oldStat}}

	tagged := func(qid, calls int64, c string) pssc.Stat {
		return pssc.Stat{UserID: 1, DBID: 2, QueryID: qid, TopLevel: true, Tags: map[string]string{"controller": c}, Calls: calls, StatsSince: time.Unix(100, 0)}
	}
	oldA, curA := tagged(7, 4, "a"), tagged(7, 6, "a")
	curB := tagged(7, 1, "b") // new since prev
	oldOther, curOther := tagged(8, 1, "o"), tagged(8, 5, "o")
	psscPrev := pssc.Snapshot{Entries: map[pssc.Key]pssc.Stat{pssc.KeyOf(oldA): oldA, pssc.KeyOf(oldOther): oldOther}}
	psscDeltas, psscNext := pssc.Diff(psscPrev, []pssc.Stat{curA, curB, curOther})
	pc := newPSSCContexts(true, psscDeltas, psscPrev, psscNext, time.Unix(150, 0))

	failing := &sequenceTextFiller{failures: textFetchAttempts, texts: map[int64]string{7: "select 7"}}
	_, carried, carriedPSSC := w.buildHarvestBatchAndSnapshotPSSC(context.Background(), failing, prev, next, deltas, pc, time.Unix(150, 0), time.Unix(160, 0))
	if carried.Entries[key].Calls != 10 {
		t.Fatalf("pgss carried calls = %d, want 10", carried.Entries[key].Calls)
	}
	if len(carriedPSSC.Entries) != 2 || carriedPSSC.Entries[pssc.KeyOf(oldA)].Calls != 4 || carriedPSSC.Entries[pssc.KeyOf(curOther)].Calls != 5 {
		t.Fatalf("pssc next = %+v, want a carried at 4, b dropped, other fresh at 5", carriedPSSC.Entries)
	}
}

// pssc read a call pgss hadn't yet (read skew): the excess isn't shipped
// this window, and stays in the pssc snapshot so the next window, where pgss
// catches up, still attributes it instead of shipping it untagged.
func TestPSSCExcessIsHeldForTheNextWindow(t *testing.T) {
	w := New(Config{LogicalID: 7, PhysicalID: 42}, RealClock{})
	key := pgss.Key{UserID: 1, DBID: 2, TopLevel: true, QueryID: 7}
	texts := &sequenceTextFiller{texts: map[int64]string{7: "select 7"}}
	a := func(calls int64) pssc.Stat {
		return pssc.Stat{UserID: 1, DBID: 2, QueryID: 7, TopLevel: true, Tags: map[string]string{"controller": "a"},
			Calls: calls, ExecTime: float64(calls), StatsSince: time.Unix(100, 0)}
	}
	window := func(prevPSSC pssc.Snapshot, cur pssc.Stat, pgssCalls int64) (map[[3]string]uint64, pssc.Snapshot) {
		deltas, next := pssc.Diff(prevPSSC, []pssc.Stat{cur})
		pc := newPSSCContexts(true, deltas, prevPSSC, next, time.Unix(150, 0))
		d := pgss.Delta{Stat: pgss.Stat{UserID: 1, DBID: 2, TopLevel: true, QueryID: 7, Calls: pgssCalls, TotalExecTime: float64(pgssCalls)}}
		snap := pgss.Snapshot{Entries: map[pgss.Key]pgss.Stat{key: d.Stat}}
		batch, _, psscNext := w.buildHarvestBatchAndSnapshotPSSC(context.Background(), texts, snap, snap, []pgss.Delta{d}, pc, time.Unix(150, 0), time.Unix(160, 0))
		return contextCounts(batch.GetAggregates()[0]), psscNext
	}
	logs := captureLog(t)
	prev := pssc.Snapshot{Entries: map[pssc.Key]pssc.Stat{pssc.KeyOf(a(10)): a(10)}}
	first, next := window(prev, a(15), 4)
	if n := strings.Count(logs.String(), "held back"); n != 1 {
		t.Fatalf("want one summary line with \"held back\", got %d: %s", n, logs.String())
	}
	if len(first) != 1 || first[[3]string{"a", "", ""}] != 4 {
		t.Fatalf("first window = %v, want a:4", first)
	}
	second, _ := window(next, a(15), 1)
	if len(second) != 1 || second[[3]string{"a", "", ""}] != 1 {
		t.Fatalf("second window = %v, want a:1 (the held call), no untagged", second)
	}
}

// The held backlog is a reflection of D_n = pssc − pgss cumulative calls,
// so with read skew bounded by k it stays bounded, not a random walk. 1000
// windows of a busy, fully tagged query, pssc reading 0..k calls ahead of
// pgss at each read.
func TestPSSCHeldBacklogStaysBoundedUnderReadSkew(t *testing.T) {
	for _, behind := range []bool{false, true} {
		t.Run(fmt.Sprintf("pssc_can_lag=%v", behind), func(t *testing.T) { heldBacklogSim(t, behind) })
	}
}

// heldBacklogSim: with behind, pssc may also read up to k calls behind pgss.
func heldBacklogSim(t *testing.T, behind bool) {
	captureLog(t)
	const k, windows = 5, 1000
	w := New(Config{LogicalID: 7, PhysicalID: 42}, RealClock{})
	key := pgss.Key{UserID: 1, DBID: 2, TopLevel: true, QueryID: 7}
	texts := &sequenceTextFiller{texts: map[int64]string{7: "select 7"}}
	a := func(calls int64) pssc.Stat {
		return pssc.Stat{UserID: 1, DBID: 2, QueryID: 7, TopLevel: true, Tags: map[string]string{"controller": "a"},
			Calls: calls, ExecTime: float64(calls), StatsSince: time.Unix(100, 0)}
	}
	rng := uint64(12345)
	rnd := func(n int64) int64 { // xorshift, deterministic
		rng ^= rng << 13
		rng ^= rng >> 7
		rng ^= rng << 17
		return int64(rng % uint64(n+1))
	}
	calls := make([]int64, windows+1)
	for i := range calls {
		calls[i] = 10 + rnd(20)
	}
	prevPSSC := pssc.Snapshot{Entries: map[pssc.Key]pssc.Stat{pssc.KeyOf(a(0)): a(0)}}
	var truth, shippedTagged, shippedAll, maxHeld, maxLag int64
	for n := 0; n < windows; n++ {
		truth += calls[n]
		skew := rnd(k)
		if behind {
			skew = rnd(2*k) - k
		}
		if skew > calls[n+1] {
			skew = calls[n+1]
		}
		cur := a(truth + skew) // pssc reads after pgss
		deltas, next := pssc.Diff(prevPSSC, []pssc.Stat{cur})
		pc := newPSSCContexts(true, deltas, prevPSSC, next, time.Unix(150, 0))
		d := pgss.Delta{Stat: pgss.Stat{UserID: 1, DBID: 2, TopLevel: true, QueryID: 7, Calls: calls[n], TotalExecTime: float64(calls[n])}}
		snap := pgss.Snapshot{Entries: map[pgss.Key]pgss.Stat{key: d.Stat}}
		batch, _, psscNext := w.buildHarvestBatchAndSnapshotPSSC(context.Background(), texts, snap, snap, []pgss.Delta{d}, pc, time.Unix(150, 0), time.Unix(160, 0))
		for ctx, c := range contextCounts(batch.GetAggregates()[0]) {
			shippedAll += int64(c)
			if ctx[0] == "a" {
				shippedTagged += int64(c)
			}
		}
		held := cur.Calls - psscNext.Entries[pssc.KeyOf(cur)].Calls
		maxHeld = max(maxHeld, held)
		maxLag = max(maxLag, truth-shippedTagged)
		prevPSSC = psscNext
	}
	if shippedAll != truth {
		t.Fatalf("shipped %d calls in all, want pgss's %d", shippedAll, truth)
	}
	if maxHeld > 2*k || maxLag > 2*k {
		t.Fatalf("max held %d, max tagged lag %d; want both <= %d", maxHeld, maxLag, 2*k)
	}
	t.Logf("over %d windows: max held %d, max tagged lag %d (k=%d)", windows, maxHeld, maxLag, k)
}

// The known cost of the empty-snapshot rule: when the previous pssc
// snapshot was legitimately empty (pssc had no entries yet), an entry whose
// stats_since predates the previous harvest (as the observed server's
// clock has it) ships untagged for that one window.
func TestPSSCEmptyPreviousSnapshotLosesOneWindowOfAttribution(t *testing.T) {
	captureLog(t)
	d := psscDelta(7, 3, 3, map[string]string{"controller": "a"})
	d.New = true
	d.StatsSince = time.Unix(149, 0) // server clock a second behind
	pc := newPSSCContexts(true, []pssc.Delta{d}, pssc.Snapshot{Entries: map[pssc.Key]pssc.Stat{}}, pssc.Snapshot{}, time.Unix(150, 0))
	counts, _ := pc.forDelta(pgssDelta(7, 3, 3))
	if len(counts) != 1 || counts[untaggedContextKey] != 3 {
		t.Fatalf("counts = %v, want untagged 3 (the one-window loss)", counts)
	}
}

func TestCapContextsFoldsSmallestIntoUntagged(t *testing.T) {
	captureLog(t)
	events := map[string]QueryEvent{
		"f1": {context: map[string]uint64{"a": 10, "b": 1, untaggedContextKey: 2}, context_time: map[string]float64{"a": 1, "b": 5, untaggedContextKey: 1}},
		"f2": {context: map[string]uint64{"c": 7, "d": 3}, context_time: map[string]float64{"c": 1, "d": 2}},
	}
	// Five contexts, limit four: two events reserve two slots, so the two
	// biggest tagged contexts (a, c) keep theirs.
	capContexts(events, 4)
	total := 0
	for _, e := range events {
		total += len(e.context)
	}
	if total != 4 {
		t.Fatalf("contexts = %d, want 4: %+v", total, events)
	}
	f1, f2 := events["f1"], events["f2"]
	if f1.context["a"] != 10 || f1.context[untaggedContextKey] != 3 || f1.context_time[untaggedContextKey] != 6 {
		t.Fatalf("f1 = %+v, want b folded into untagged", f1)
	}
	if _, ok := f1.context["b"]; ok {
		t.Fatalf("f1 still has b: %+v", f1)
	}
	if f2.context["c"] != 7 || f2.context[untaggedContextKey] != 3 || f2.context_time[untaggedContextKey] != 2 {
		t.Fatalf("f2 = %+v, want c kept and d folded into a new untagged", f2)
	}
	// Under the limit: untouched.
	small := map[string]QueryEvent{"f": {context: map[string]uint64{"a": 1, "b": 1}}}
	capContexts(small, 2)
	if len(small["f"].context) != 2 {
		t.Fatalf("small = %+v, want untouched", small)
	}
}
