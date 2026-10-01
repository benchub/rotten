package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Retention is how many days pg_partman keeps partitions of rotten.events
// and rotten.event_context before it drops them.
type Retention int

const (
	// DefaultRetention is what migrate sets when it isn't told otherwise.
	DefaultRetention Retention = 21
	// MinRetention and MaxRetention bound what migrate accepts.
	MinRetention Retention = 1
	MaxRetention Retention = 3650
)

// retentionTables are the pg_partman parents whose retention migrate sets.
var retentionTables = []string{"rotten.events", "rotten.event_context"}

var (
	pgDays = regexp.MustCompile(`^(\d{1,9})\s*(?:days?|d)$`)
)

// ParseRetention accepts a whole number of days, written as a Postgres
// interval ("21 days", "1 day"), with a day suffix ("21d"), or as a Go
// duration that's a whole number of days ("504h"). The result must be
// between MinRetention and MaxRetention.
func ParseRetention(s string) (Retention, error) {
	in := strings.ToLower(strings.TrimSpace(s))
	var days int64
	if m := pgDays.FindStringSubmatch(in); m != nil {
		n, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("retention %q: %w", s, err)
		}
		days = n
	} else if d, err := time.ParseDuration(in); err == nil && in != "0" {
		if d%(24*time.Hour) != 0 {
			return 0, fmt.Errorf("retention %q: must be a whole number of days", s)
		}
		days = int64(d / (24 * time.Hour))
	} else {
		return 0, fmt.Errorf("retention %q: want days, like \"21 days\" or \"21d\"", s)
	}
	r := Retention(days)
	if err := r.validate(); err != nil {
		return 0, fmt.Errorf("retention %q: %w", s, err)
	}
	return r, nil
}

func (r Retention) validate() error {
	if r < MinRetention || r > MaxRetention {
		return fmt.Errorf("retention must be between %d and %d days, got %d", MinRetention, MaxRetention, int(r))
	}
	return nil
}

// String renders r as a Postgres interval, the form part_config stores.
func (r Retention) String() string {
	if r == 1 {
		return "1 day"
	}
	return fmt.Sprintf("%d days", int(r))
}

// applyRetention sets part_config.retention for both partitioned tables,
// passing the interval as a bound parameter.
func applyRetention(ctx context.Context, tx *sql.Tx, r Retention) error {
	for _, table := range retentionTables {
		res, err := tx.ExecContext(ctx,
			`update public.part_config set retention = $1, retention_keep_table = false
			 where parent_table = $2`, r.String(), table)
		if err != nil {
			return fmt.Errorf("migrate: retention for %s: %w", table, err)
		}
		if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("migrate: retention for %s: %w", table, err)
		} else if n != 1 {
			return fmt.Errorf("migrate: retention for %s: part_config has %d rows, want 1", table, n)
		}
	}
	return nil
}
