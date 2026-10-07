package worker

import (
	"log"
	"math"
	"sort"
	"time"

	"github.com/benchub/rotten/internal/pgss"
	"github.com/benchub/rotten/internal/pssc"
)

// Contexts come from pg_stat_statement_context (pssc), never from query
// text. For each pgss entry the harvest ships, its pssc deltas with the same
// (userid, dbid, queryid, toplevel) become contexts: the controller,
// action, and job (or job_tag) tags map to controller, action, and job tag, and other
// tags are ignored (so tag sets that differ only in other keys are summed,
// and a tag set with none of the three counts as untagged). The calls pgss
// counted that pssc didn't attribute become the untagged context (all three
// empty). Without pssc, every call is untagged.
//
// pgss and pssc are read one after the other, so calls that finish between
// the two reads are in one view and not the other. That shows up as a pssc
// sum slightly above pgss's calls in one window. forDelta ships only pgss's
// calls and holdBack keeps the rest in the pssc snapshot, so they're
// attributed in the next window, when pgss has counted them. Calls pgss
// counted before pssc did ship as untagged in that window (pssc's later
// count of them is then held against future pgss calls), so the untagged
// share can run slightly high, by about the calls in flight at a read.

// cappedTagValue is what a context field holds when pssc reports the tag
// value as capped (pssc.Capped: the key reached cardinality_cap). The calls
// are real and tagged, just not distinguishable by value, so they ship as
// their own context rather than as untagged. The server can't store
// pssc.Capped itself (it rejects NUL bytes).
const cappedTagValue = "(capped)"

// contextKey is the key of a context in QueryEvent.context.
func contextKey(controller, action, jobTag string) string {
	return controller + "\x00" + action + "\x00" + jobTag
}

var untaggedContextKey = contextKey("", "", "")

// psscContexts is one harvest's pssc deltas, indexed by pgss key.
type psscContexts struct {
	available bool
	byKey     map[pgss.Key][]pssc.Delta
	// prev is the saved pssc snapshot and next the one to save.
	prev, next pssc.Snapshot
	// held is what forDelta didn't ship per pssc entry because pssc counted
	// more than pgss (shared across copies; see holdBack).
	held map[pssc.Key]heldBack
}

type heldBack struct {
	calls int64
	time  float64
}

// holdBack lowers next's entries by what forDelta held back, so the next
// window's pssc delta includes those calls when pgss has caught up with
// them, instead of the read skew turning them into untagged calls. An entry
// stays at or above what was shipped from it, so Diff never sees it go down
// from a real read.
func holdBack(next pssc.Snapshot, held map[pssc.Key]heldBack) pssc.Snapshot {
	for k, h := range held {
		if e, ok := next.Entries[k]; ok {
			e.Calls -= h.calls
			e.ExecTime -= h.time
			next.Entries[k] = e
		}
	}
	return next
}

// newPSSCContexts indexes deltas by pgss key. When prev is empty, a delta
// that's New (no usable previous entry) but whose entry started before
// prevTakenAt is skipped: its
// values reach back before this window, which happens when the last harvest
// couldn't read pssc (so the saved pssc snapshot is empty). Its calls fall
// into the untagged context instead of shipping lifetime counts. pssc's
// stats_since is the observed server's clock and prevTakenAt the worker's;
// skew between them only moves calls between a context and untagged.
func newPSSCContexts(available bool, deltas []pssc.Delta, prev, next pssc.Snapshot, prevTakenAt time.Time) psscContexts {
	pc := psscContexts{available: available, byKey: map[pgss.Key][]pssc.Delta{}, prev: prev, next: next,
		held: map[pssc.Key]heldBack{}}
	skipped := 0
	// Only when the saved snapshot is empty: a non-empty one means pssc was
	// read last harvest, so every New entry really is new, whatever the two
	// clocks say. (An empty one can also mean pssc simply had no entries;
	// then a New entry older than prevTakenAt is still treated as stale.)
	prevEmpty := len(prev.Entries) == 0
	for _, d := range deltas {
		if prevEmpty && d.New && d.StatsSince.Before(prevTakenAt) {
			skipped++
			continue
		}
		k := pgss.Key{UserID: d.UserID, DBID: d.DBID, QueryID: d.QueryID, TopLevel: d.TopLevel}
		pc.byKey[k] = append(pc.byKey[k], d)
	}
	if skipped > 0 {
		log.Println(skipped, "pg_stat_statement_context entries have no previous snapshot but started before this window, so their calls are untagged this window")
	}
	return pc
}

// carrySkippedTextPSSC is carrySkippedTextSnapshot for pssc: for each pgss
// key whose snapshot entry was carried, next gets prev's pssc entries for
// that key in place of the fresh ones, so next window's pgss and pssc
// deltas cover the same span. A tag set new since prev is left out of next,
// so it counts in full (as New) next window.
func carrySkippedTextPSSC(prev, next pssc.Snapshot, carried map[pgss.Key]bool) pssc.Snapshot {
	if len(carried) == 0 || next.Entries == nil {
		return next
	}
	out := pssc.Snapshot{Entries: make(map[pssc.Key]pssc.Stat, len(next.Entries))}
	pk := func(k pssc.Key) pgss.Key {
		return pgss.Key{UserID: k.UserID, DBID: k.DBID, QueryID: k.QueryID, TopLevel: k.TopLevel}
	}
	for k, e := range next.Entries {
		if !carried[pk(k)] {
			out.Entries[k] = e
		}
	}
	for k, e := range prev.Entries {
		if carried[pk(k)] {
			out.Entries[k] = e
		}
	}
	return out
}

func tagValue(tags map[string]string, key string) string {
	v := tags[key]
	if v == pssc.Capped {
		return cappedTagValue
	}
	return v
}

// jobTagValue is the job tag: the `job` tag, or `job_tag` when there's no
// `job` (the dev traffic generator's job marginalia, and what the old
// sample regex `job(_tag)?` accepted).
func jobTagValue(tags map[string]string) string {
	if _, ok := tags["job"]; ok {
		return tagValue(tags, "job")
	}
	return tagValue(tags, "job_tag")
}

// forDelta returns the context counts and execution times (ms) for one pgss
// delta. The untagged context gets the calls and exec time pgss counted
// beyond pssc's sum, clamped at zero. When pssc's calls exceed pgss's (the
// read skew, or resets landing between the reads), that's logged and the
// tagged counts and times are scaled down to fit pgss's calls, since the
// server rejects contexts that sum past the aggregate's calls.
func (pc psscContexts) forDelta(d pgss.Delta) (map[string]uint64, map[string]float64) {
	calls := wholeCount(float64(d.Calls))
	counts := map[string]uint64{}
	times := map[string]float64{}
	// Per pssc entry first (keyed by its canonical tags, unique within one
	// pgss key), so a scale-down knows what each entry didn't ship.
	entries := pc.byKey[pgss.KeyOf(d.Stat)]
	ec := map[string]uint64{}
	et := map[string]float64{}
	var sum uint64
	for _, p := range entries {
		k := pssc.CanonicalTags(p.Tags)
		ec[k] = wholeCount(float64(p.Calls))
		et[k] = p.ExecTime
		sum += ec[k]
	}
	if sum > calls {
		orig := make(map[string]uint64, len(ec))
		origT := make(map[string]float64, len(et))
		for k, c := range ec {
			orig[k], origT[k] = c, et[k]
		}
		scaleCounts(ec, et, calls, sum)
		sum = calls
		if pc.held != nil {
			for _, p := range entries {
				k := pssc.CanonicalTags(p.Tags)
				pc.held[pssc.KeyOf(p.Stat)] = heldBack{calls: int64(orig[k] - ec[k]), time: origT[k] - et[k]}
			}
		}
	}
	for _, p := range entries {
		e := pssc.CanonicalTags(p.Tags)
		k := contextKey(tagValue(p.Tags, "controller"), tagValue(p.Tags, "action"), jobTagValue(p.Tags))
		counts[k] += ec[e]
		times[k] += et[e]
	}
	var tagged float64
	for k, c := range counts {
		if c == 0 {
			delete(counts, k)
			delete(times, k)
			continue
		}
		tagged += times[k]
	}
	if rest := calls - sum; rest > 0 {
		counts[untaggedContextKey] += rest
		times[untaggedContextKey] += math.Max(0, d.TotalExecTime-tagged)
	}
	return counts, times
}

// scaleCounts scales counts (summing to sum) down to sum to exactly calls,
// by largest remainder with ties broken by key, and scales times by the
// same factor. Flooring alone would push the lost calls into untagged.
// Counts come from wholeCount, so each is at most MaxContextCount (2^53),
// where float64 is still exact; when calls itself sits at that cap (pgss's
// count was capped), the scaled counts can't exceed it either.
func scaleCounts(counts map[string]uint64, times map[string]float64, calls, sum uint64) {
	f := float64(calls) / float64(sum)
	type rem struct {
		key  string
		frac float64
	}
	var rems []rem
	var got uint64
	for k, c := range counts {
		exact := float64(c) * f
		fl := math.Floor(exact)
		counts[k] = uint64(fl)
		times[k] *= f
		got += counts[k]
		rems = append(rems, rem{k, exact - fl})
	}
	sort.Slice(rems, func(i, j int) bool {
		if rems[i].frac != rems[j].frac {
			return rems[i].frac > rems[j].frac
		}
		return rems[i].key < rems[j].key
	})
	for i := 0; got < calls && i < len(rems); i++ {
		counts[rems[i].key]++
		got++
	}
}

// capContexts keeps a harvest within harvestlimits.MaxHarvestContexts. Each
// event keeps room for its untagged context; the tagged contexts with the
// most calls across the harvest keep theirs, and the rest fold into their
// event's untagged context. The reserve is conservative: it holds a slot
// even for an event that has no untagged context and loses nothing to the
// fold, so a capped harvest can end a few contexts under the limit.
func capContexts(events map[string]QueryEvent, limit int) {
	total := 0
	for _, e := range events {
		total += len(e.context)
	}
	if total <= limit {
		return
	}
	type entry struct {
		fp, key string
		count   uint64
	}
	var tagged []entry
	for fp, e := range events {
		for k, c := range e.context {
			if k != untaggedContextKey {
				tagged = append(tagged, entry{fp, k, c})
			}
		}
	}
	sort.Slice(tagged, func(i, j int) bool {
		if tagged[i].count != tagged[j].count {
			return tagged[i].count > tagged[j].count
		}
		if tagged[i].fp != tagged[j].fp {
			return tagged[i].fp < tagged[j].fp
		}
		return tagged[i].key < tagged[j].key
	})
	keep := limit - len(events)
	if keep < 0 {
		keep = 0
	}
	if keep > len(tagged) {
		keep = len(tagged)
	}
	folded := 0
	for _, t := range tagged[keep:] {
		e := events[t.fp]
		e.context[untaggedContextKey] += e.context[t.key]
		if e.context_time != nil {
			e.context_time[untaggedContextKey] += e.context_time[t.key]
			delete(e.context_time, t.key)
		}
		delete(e.context, t.key)
		folded++
	}
	log.Printf("harvest has %d contexts, over the limit of %d; folded the %d smallest into untagged", total, limit, folded)
}
