package pgss

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DefaultMinmaxResetSchema is where schema/observer.sql puts the min/max
// reset wrapper unless told otherwise.
const DefaultMinmaxResetSchema = "rotten"

// ErrNoMinmaxReset means the extension is older than 1.11 (Postgres 17), so
// there's no min/max only reset.
var ErrNoMinmaxReset = errors.New("pgss: min/max reset needs Postgres 17+")

// MinmaxReset calls <schema>.pg_stat_statements_minmax_reset(), the wrapper
// from schema/observer.sql, which resets only min and max, not the counters.
// If the wrapper or its schema is missing, the error says to run
// schema/observer.sql or check the MinmaxResetSchema setting.
// An empty schema means DefaultMinmaxResetSchema. On 14 through 16 it returns
// ErrNoMinmaxReset without calling the reset. It may still read extversion.
func (r *Reader) MinmaxReset(ctx context.Context, schema string) error {
	has, err := r.HasMinmaxReset(ctx)
	if err != nil {
		return err
	}
	if !has {
		return ErrNoMinmaxReset
	}
	if schema == "" {
		schema = DefaultMinmaxResetSchema
	}
	q := "select " + pgx.Identifier{schema, "pg_stat_statements_minmax_reset"}.Sanitize() + "()"
	if _, err := r.conn.Exec(ctx, q); err != nil {
		var pgErr *pgconn.PgError
		// 42883: no such function. 3F000: no such schema.
		if errors.As(err, &pgErr) && (pgErr.Code == "42883" || pgErr.Code == "3F000") {
			return fmt.Errorf("pgss: min/max reset: %s.pg_stat_statements_minmax_reset() not found; run schema/observer.sql, or check the MinmaxResetSchema setting: %w", pgx.Identifier{schema}.Sanitize(), err)
		}
		return fmt.Errorf("pgss: min/max reset: %w", err)
	}
	return nil
}

// WindowMinMax returns the exec-time min and max to report for d's window,
// and whether they're lifetime values rather than window-only ones.
//
// min and max can't be diffed, so they're always the current values. What
// changes is how far back they reach:
//
//   - New: the counters are the entry's full values, so min and max cover
//     the same span as the counters. lifetime is false.
//   - 17+ and MinmaxStatsSince moved past the snapshot's value: a min/max
//     reset ran since the previous harvest, so they cover this window only.
//     lifetime is false.
//   - Otherwise (14 through 16, which have no minmax_stats_since, or 17+ with
//     no reset since the previous harvest): they reach back before the
//     window. lifetime is true.
//
// "After the snapshot's value" stands in for "after the previous harvest":
// the snapshot holds what that harvest read, and any later reset moves it
// forward.
func WindowMinMax(d Delta) (min, max float64, lifetime bool) {
	min, max = d.MinTime, d.MaxTime
	if d.New || d.Prev == nil {
		return min, max, false
	}
	cur, old := d.MinmaxStatsSince, d.Prev.MinmaxStatsSince
	if cur != nil && old != nil && cur.After(*old) {
		return min, max, false
	}
	return min, max, true
}
