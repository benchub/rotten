package pssc

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestNotAvailable(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{&pgconn.PgError{Code: "42P01"}, true},
		{fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "3F000"}), true},
		// Library loaded without shared memory (e.g. session_preload_libraries).
		// The text comes from pssc's docs/sql-interface.md.
		{&pgconn.PgError{Code: "55000", Message: `pg_stat_statement_context must be loaded via "shared_preload_libraries"`}, true},
		{&pgconn.PgError{Code: "XX000", Message: "x", Hint: "add it to shared_preload_libraries"}, false},
		// pg_stat_statements' own error is a real failure, not missing pssc.
		{&pgconn.PgError{Code: "55000", Message: `pg_stat_statements must be loaded via "shared_preload_libraries"`}, false},
		{&pgconn.PgError{Code: "42501", Message: "permission denied"}, false},
		{errors.New("conn closed"), false},
	}
	for _, c := range cases {
		if got := notAvailable(c.err); got != c.want {
			t.Errorf("notAvailable(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}
