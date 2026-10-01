package fingerprint

import (
	"errors"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

var errFakeDeparse = errors.New("fake deparse failure")

// No real query reaches the deparse-failure branch, so these tests call
// deparseFallback directly with a stand-in deparse error.

func TestDeparseFallbackFingerprintsRawQuery(t *testing.T) {
	query := "SELECT a FROM users WHERE id = 1"
	want, err := pg_query.Fingerprint(query)
	if err != nil {
		t.Fatalf("pg_query.Fingerprint: %v", err)
	}
	got, err := deparseFallback(query, errFakeDeparse)
	if err != nil {
		t.Fatalf("deparseFallback returned error: %v", err)
	}
	if got != want {
		t.Errorf("deparseFallback = %q, want %q", got, want)
	}
}

func TestDeparseFallbackFingerprintFailure(t *testing.T) {
	got, err := deparseFallback("SELEC nonsense ((", errFakeDeparse)
	if err == nil || err.Error() != "failed to deparse and fingerprint fallback" {
		t.Errorf("err = %v, want fingerprint fallback failure", err)
	}
	if got != "" {
		t.Errorf("fingerprint = %q, want empty", got)
	}
}

func TestDeparseFallbackRefusesCursorAndTempTable(t *testing.T) {
	cases := map[string]string{
		"cursor":     "FETCH 10 FROM users_cursor_abc123",
		"temp table": "SELECT * FROM orders_temp_table_abc123",
	}
	for name, query := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := deparseFallback(query, errFakeDeparse)
			if err == nil || err.Error() != "failed to deparse; no fingerprint fallback" {
				t.Errorf("err = %v, want refusal", err)
			}
			if got != "" {
				t.Errorf("fingerprint = %q, want empty", got)
			}
		})
	}
}
