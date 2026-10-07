package pssc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Detection is what Reader.Detect found.
type Detection struct {
	// Preloaded: the pg_stat_statement_context library is loaded (it must be
	// in shared_preload_libraries), as shown by its settings in pg_settings.
	Preloaded bool
	// Created: the extension exists in this database, in Schema.
	Created bool
	Schema  string
}

// Available reports whether pssc can be read: preloaded and created.
func (d Detection) Available() bool { return d.Preloaded && d.Created }

// Reader reads pg_stat_statement_context_totals on one connection.
type Reader struct {
	conn *pgx.Conn
}

func NewReader(conn *pgx.Conn) *Reader { return &Reader{conn: conn} }

// Detect looks up whether pssc is preloaded and created, and its schema. A
// missing library or extension isn't an error.
func (r *Reader) Detect(ctx context.Context) (Detection, error) {
	var d Detection
	var schema *string
	// The observer can't read shared_preload_libraries (that needs
	// pg_read_all_settings), so look for pssc's GUCs instead: pg_settings
	// lists them only once the library has loaded and defined them, not as
	// placeholders from a config file.
	err := r.conn.QueryRow(ctx, `
select exists (select from pg_settings where name like 'pg\_stat\_statement\_context.%'),
       (select n.nspname from pg_extension e join pg_namespace n on n.oid = e.extnamespace
         where e.extname = 'pg_stat_statement_context')`).Scan(&d.Preloaded, &schema)
	if err != nil {
		return d, fmt.Errorf("pssc: detect: %w", err)
	}
	if schema != nil {
		d.Created, d.Schema = true, *schema
	}
	return d, nil
}

// ReadStats returns every row of pg_stat_statement_context_totals. ok is
// false, with no error, when pssc isn't available. It detects pssc on every
// call (one cheap catalog query), so an extension created, dropped, or moved
// since the last call is picked up. Two errors also count as not available
// rather than failures: the view or its schema vanishing between detection
// and the read (42P01, 3F000), and pssc refusing to run because its shared
// memory isn't set up ("pg_stat_statement_context must be loaded via ..."). The
// latter is what to expect when the library is loaded some other way, say
// session_preload_libraries: its settings show up, so Detect says
// preloaded, but its store doesn't exist. Rows whose queryid or
// tags are hidden (NULL, for a caller without pg_read_all_stats) are
// skipped. A JSON null tag value (a cardinality cap) becomes Capped.
func (r *Reader) ReadStats(ctx context.Context) (stats []Stat, ok bool, err error) {
	d, err := r.Detect(ctx)
	if err != nil || !d.Available() {
		return nil, false, err
	}
	q := `select userid, dbid, queryid, toplevel, tags::text, calls_total, exec_time_total, stats_since
	        from ` + pgx.Identifier{d.Schema, "pg_stat_statement_context_totals"}.Sanitize() + `
	       where queryid is not null and tags is not null`
	rows, err := r.conn.Query(ctx, q)
	if err != nil {
		if notAvailable(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("pssc: read: %w", err)
	}
	stats, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (Stat, error) {
		var s Stat
		var tags string
		var since time.Time
		if err := row.Scan(&s.UserID, &s.DBID, &s.QueryID, &s.TopLevel, &tags, &s.Calls, &s.ExecTime, &since); err != nil {
			return s, err
		}
		s.StatsSince = since.UTC()
		var raw map[string]*string
		if err := json.Unmarshal([]byte(tags), &raw); err != nil {
			return s, fmt.Errorf("pssc: tags %s: %w", tags, err)
		}
		s.Tags = make(map[string]string, len(raw))
		for k, v := range raw {
			if v == nil {
				s.Tags[k] = Capped
			} else {
				s.Tags[k] = *v
			}
		}
		return s, nil
	})
	if err != nil {
		if notAvailable(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("pssc: read: %w", err)
	}
	return stats, true, nil
}

// notAvailable reports whether err means pssc went away or can't run, not
// that the read failed. See ReadStats.
func notAvailable(err error) bool {
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		return false
	}
	switch pe.Code {
	case "42P01", "3F000": // undefined_table, invalid_schema_name
		return true
	}
	// pssc's own error (docs/sql-interface.md). Matching just
	// "shared_preload_libraries" would also swallow pg_stat_statements'.
	return strings.Contains(pe.Message, "pg_stat_statement_context must be loaded via")
}
