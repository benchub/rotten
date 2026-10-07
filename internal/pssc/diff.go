// Package pssc reads pg_stat_statement_context
// (https://github.com/benchub/pg_stat_statement_context), which splits
// pg_stat_statements entries by the tags in each statement's comments. It's
// optional on an observed database: without it, Reader.ReadStats reports
// "not available" rather than an error.
package pssc

import (
	"encoding/json"
	"sort"
	"time"
)

// Capped is the tag value Stat.Tags holds where pssc reports JSON null,
// which it does only when the key reached
// pg_stat_statement_context.cardinality_cap. It holds a NUL byte, which no
// Postgres text value (and so no client-supplied tag) can contain, so a
// capped value never collides with a real one, the empty string included.
// Capped entries are kept, not dropped: they are real calls whose value pssc
// no longer distinguishes.
const Capped = "\x00capped"

// Stat is one row of pg_stat_statement_context_totals.
type Stat struct {
	UserID   uint32
	DBID     uint32
	QueryID  int64
	TopLevel bool
	Tags     map[string]string

	// Calls and ExecTime are calls_total and exec_time_total: counters over
	// the entry's life, which start over (with a new StatsSince) when the
	// entry is reset or evicted and created again.
	Calls      int64
	ExecTime   float64
	StatsSince time.Time
}

// Key identifies one pssc entry. Tags is the canonical tag set (see
// CanonicalTags), so equal tag maps give equal keys.
type Key struct {
	UserID   uint32
	DBID     uint32
	QueryID  int64
	TopLevel bool
	Tags     string
}

// KeyOf returns the snapshot key of s.
func KeyOf(s Stat) Key {
	return Key{UserID: s.UserID, DBID: s.DBID, QueryID: s.QueryID, TopLevel: s.TopLevel, Tags: CanonicalTags(s.Tags)}
}

// CanonicalTags encodes tags as a JSON array of [key, value] pairs sorted by
// key. A nil and an empty map both encode as "[]".
func CanonicalTags(tags map[string]string) string {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	pairs := make([][2]string, len(keys))
	for i, k := range keys {
		pairs[i] = [2]string{k, tags[k]}
	}
	b, err := json.Marshal(pairs)
	if err != nil {
		panic(err) // strings always marshal
	}
	return string(b)
}

// ParseTags decodes CanonicalTags output.
func ParseTags(s string) (map[string]string, error) {
	var pairs [][2]string
	if err := json.Unmarshal([]byte(s), &pairs); err != nil {
		return nil, err
	}
	tags := make(map[string]string, len(pairs))
	for _, p := range pairs {
		tags[p[0]] = p[1]
	}
	return tags, nil
}

// Snapshot is every entry from the last read, keyed by Key.
type Snapshot struct {
	Entries map[Key]Stat
}

// Delta is one entry's activity for a window. Calls and ExecTime hold the
// change, or the full current values when New is true. Prev is the snapshot
// entry they were subtracted from, nil when New.
type Delta struct {
	Stat
	New  bool
	Prev *Stat
}

// Diff compares the current read against the previous snapshot and returns
// each entry's activity plus the snapshot to keep. It's pure and doesn't
// modify prev or cur. An entry is new (its full values count) when it's
// missing from prev, its StatsSince changed (reset, or evicted and
// created again), or Calls or ExecTime went down. There's no store-wide
// reset check: pssc's reset gives every entry a new stats_since.
//
// Entries missing from cur are dropped from next; every entry of cur goes
// into next as read. Deltas with zero calls are left out of the result but
// kept in next.
func Diff(prev Snapshot, cur []Stat) ([]Delta, Snapshot) {
	next := Snapshot{Entries: make(map[Key]Stat, len(cur))}
	var deltas []Delta
	for _, c := range cur {
		k := KeyOf(c)
		next.Entries[k] = c
		old, ok := prev.Entries[k]
		d := Delta{Stat: c}
		if !ok || !old.StatsSince.Equal(c.StatsSince) || c.Calls < old.Calls || c.ExecTime < old.ExecTime {
			d.New = true
		} else {
			o := old
			d.Prev = &o
			d.Calls -= old.Calls
			d.ExecTime -= old.ExecTime
		}
		// Dropped on purpose even when ExecTime moved: with no new calls
		// there's nothing to attribute, matching pgss.Diff.
		if d.Calls == 0 {
			continue
		}
		deltas = append(deltas, d)
	}
	return deltas, next
}
