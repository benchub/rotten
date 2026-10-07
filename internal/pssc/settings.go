package pssc

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Settings holds the pssc settings the worker checks. Both are sighup
// settings, so pg_settings shows them to every role, the observer included.
type Settings struct {
	Extractors string
	Tags       string
}

// Settings reads pg_stat_statement_context.extractors and .tags.
func (r *Reader) Settings(ctx context.Context) (Settings, error) {
	var s Settings
	err := r.conn.QueryRow(ctx, `
select coalesce((select setting from pg_settings where name = 'pg_stat_statement_context.extractors'), ''),
       coalesce((select setting from pg_settings where name = 'pg_stat_statement_context.tags'), '')`).Scan(&s.Extractors, &s.Tags)
	if err != nil {
		return s, fmt.Errorf("pssc: settings: %w", err)
	}
	return s, nil
}

// UtilityMissingQueryID reads pg_stat_statement_context_info()'s
// utility_missing_queryid: tracked utility statements pssc couldn't record
// because they had no query ID. It rising steadily usually means pssc is
// loaded before pg_stat_statements in shared_preload_libraries.
func (r *Reader) UtilityMissingQueryID(ctx context.Context, schema string) (int64, error) {
	var n int64
	q := `select utility_missing_queryid from ` + pgx.Identifier{schema, "pg_stat_statement_context_info"}.Sanitize() + `()`
	if err := r.conn.QueryRow(ctx, q).Scan(&n); err != nil {
		return 0, fmt.Errorf("pssc: info: %w", err)
	}
	return n, nil
}

// splitTop splits s on commas outside parentheses and single quotes.
func splitTop(s string) []string {
	var out []string
	depth, quoted, start := 0, false, 0
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '\'':
			quoted = !quoted
		case quoted:
		case c == '(':
			depth++
		case c == ')':
			depth--
		case c == ',' && depth == 0:
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return append(out, s[start:])
}

// SeesPrepended reports whether an extractors setting has a comment
// extractor (sqlcommenter, marginalia, or regex) that reads leading
// comments: one with position=any or position=prepend. sqlcommenter and
// marginalia default to append, which misses prepended marginalia; regex
// defaults to any.
func SeesPrepended(extractors string) bool {
	for _, ex := range splitTop(extractors) {
		ex = strings.TrimSpace(ex)
		name, params, _ := strings.Cut(ex, "(")
		switch strings.ToLower(strings.TrimSpace(name)) {
		case "sqlcommenter", "marginalia", "regex":
		default:
			continue
		}
		// regex defaults to position=any; the comment formats to append.
		position := "append"
		if strings.EqualFold(strings.TrimSpace(name), "regex") {
			position = "any"
		}
		params = strings.TrimSuffix(strings.TrimSpace(params), ")")
		for _, p := range splitTop(params) {
			k, v, ok := strings.Cut(p, "=")
			if ok && strings.EqualFold(strings.TrimSpace(k), "position") {
				position = strings.ToLower(strings.Trim(strings.TrimSpace(v), "'"))
			}
		}
		if position == "any" || position == "prepend" {
			return true
		}
	}
	return false
}

// MissingMappedTags returns the tag keys the worker maps to contexts that a
// tags allowlist leaves out: controller, action, and job (job_tag counts as
// job, since the worker falls back to it). '*' keeps every key.
func MissingMappedTags(tags string) []string {
	if strings.TrimSpace(tags) == "*" {
		return nil
	}
	have := map[string]bool{}
	for _, k := range strings.Split(tags, ",") {
		have[strings.TrimSpace(k)] = true
	}
	var missing []string
	for _, k := range []string{"controller", "action"} {
		if !have[k] {
			missing = append(missing, k)
		}
	}
	if !have["job"] && !have["job_tag"] {
		missing = append(missing, "job or job_tag")
	}
	return missing
}
