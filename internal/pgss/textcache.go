package pgss

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// TextCache holds query text by Key, so each harvest fetches text only for
// entries it hasn't seen. Reader.ReadStats returns stats without text
// (showtext := false); a harvest then calls Retain with every row it read and
// Fill with the rows it picked:
//
//	stats, _ := r.ReadStats(ctx)
//	c.Retain(stats)          // drop text for evicted keys
//	picked := topNDeltas(deltas, 100) // the rows to send
//	c.Fill(ctx, picked)      // set Query, fetching only misses
//
// A TextCache is not safe for concurrent use.
type TextCache struct {
	r       *Reader
	text    map[Key]string
	fetches int
}

// TextFetches counts text queries sent to the server. Fill sends at most one
// per call, and none when every key is cached or hidden.
func (c *TextCache) TextFetches() int { return c.fetches }

// NewTextCache returns an empty cache that fetches text through r.
func NewTextCache(r *Reader) *TextCache { return &TextCache{r: r, text: map[Key]string{}} }

// Retain drops text for every key not in all, the full set of rows from the
// current ReadStats. Those entries were evicted from pg_stat_statements.
func (c *TextCache) Retain(all []Stat) {
	live := make(map[Key]struct{}, len(all))
	for i := range all {
		live[KeyOf(all[i])] = struct{}{}
	}
	for k := range c.text {
		if _, ok := live[k]; !ok {
			delete(c.text, k)
		}
	}
}

// Has reports whether k's text is cached.
func (c *TextCache) Has(k Key) bool { _, ok := c.text[k]; return ok }

// Len returns the number of cached texts.
func (c *TextCache) Len() int { return len(c.text) }

// Fill sets Query on each of stats, in place. Keys missing from the cache are
// fetched in one pg_stat_statements(showtext := true) query, filtered to
// those keys. That query still reads the whole text file on the server, which
// is why it runs only when there are misses. A key that is gone by the time
// of the fetch keeps an empty Query and isn't cached.
//
// QueryID 0 means a NULL queryid: a row this role can't see (text
// "<insufficient privilege>"), folded with every other hidden row into one
// key. Fill never fetches or caches it, and leaves its Query empty.
func (c *TextCache) Fill(ctx context.Context, stats []Stat) error {
	var uids, dbids []uint32
	var tops []bool
	var qids []int64
	seen := map[Key]struct{}{}
	for i := range stats {
		k := KeyOf(stats[i])
		if k.QueryID == 0 {
			continue
		}
		if q, ok := c.text[k]; ok {
			stats[i].Query = q
			continue
		}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		uids, dbids, tops, qids = append(uids, k.UserID), append(dbids, k.DBID), append(tops, k.TopLevel), append(qids, k.QueryID)
	}
	if len(qids) > 0 {
		c.fetches++
		rows, err := c.r.conn.Query(ctx, `
select s.userid, s.dbid, s.toplevel, coalesce(s.queryid, 0), coalesce(s.query, '')
  from pg_stat_statements(showtext := true) s
  join unnest($1::oid[], $2::oid[], $3::bool[], $4::int8[]) k(userid, dbid, toplevel, queryid)
    on (s.userid, s.dbid, s.toplevel, coalesce(s.queryid, 0)) = (k.userid, k.dbid, k.toplevel, k.queryid)`,
			uids, dbids, tops, qids)
		if err != nil {
			return fmt.Errorf("pgss: fetch text: %w", err)
		}
		var k Key
		var q string
		_, err = pgx.ForEachRow(rows, []any{&k.UserID, &k.DBID, &k.TopLevel, &k.QueryID, &q}, func() error {
			c.text[k] = q
			return nil
		})
		if err != nil {
			return fmt.Errorf("pgss: fetch text: %w", err)
		}
	}
	for i := range stats {
		stats[i].Query = c.text[KeyOf(stats[i])]
	}
	return nil
}
