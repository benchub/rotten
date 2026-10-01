package fingerprint

import (
	"io"
	"log"
	"os"
	"strings"
	"testing"
)

// TestConfiguredTempTablePattern pins that a configured temp-table pattern
// collapses the short-suffix corpus case (temp_table_short_suffix) that the
// default leaves alone.
func TestConfiguredTempTablePattern(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stderr)

	short := "SELECT * FROM users_temp_table_abc WHERE id = 5"
	long := "SELECT * FROM users_temp_table_qwerty WHERE id = 6"

	fp := func(q string, opts Options) string {
		t.Helper()
		got, err := Normalized(q, opts)
		if err != nil {
			t.Fatalf("Normalized(%q): %v", q, err)
		}
		return got
	}

	if fp(short, Options{}) == fp(long, Options{}) {
		t.Fatalf("default pattern already collapses the short suffix")
	}

	opts, err := NewOptions(false, "", `([^\s]+)_temp_table_[0-9a-z]{3}[0-9a-z]*([^\s]*)`)
	if err != nil {
		t.Fatal(err)
	}
	if a, b := fp(short, opts), fp(long, opts); a != b {
		t.Errorf("configured pattern: short %s, long %s, want equal", a, b)
	}
}

// TestDefaultPatternsMatchExplicitDefaults pins that the zero Options and
// explicitly configured defaults behave the same.
func TestDefaultPatternsMatchExplicitDefaults(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stderr)
	opts, err := NewOptions(false, DefaultCursorPattern, DefaultTempTablePattern)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"FETCH 10 FROM users_cursor_ab12",
		`DECLARE "users_cursor_ab12" CURSOR FOR SELECT 1`,
		"SELECT * FROM users_temp_table_qwerty",
	} {
		a, errA := Normalized(q, Options{})
		b, errB := Normalized(q, opts)
		if a != b || (errA == nil) != (errB == nil) {
			t.Errorf("%q: zero %s/%v, explicit %s/%v", q, a, errA, b, errB)
		}
	}
}

func TestNewOptionsRejectsBadPatterns(t *testing.T) {
	for _, tc := range []struct{ cursor, temp, want string }{
		{"(", "", "CursorPattern"},
		{"", "[", "TempTablePattern"},
		{"cursor_[0-9]+", "", "two capture groups"},
		{"(a)(b)(c)", "", "exactly two capture groups"},
		{"", "(a)(b)(c)", "TempTablePattern"},
		{"()()", "", "empty string"},
		{"", "()()", "empty string"},
	} {
		_, err := NewOptions(false, tc.cursor, tc.temp)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("NewOptions(%q, %q) = %v, want error mentioning %q", tc.cursor, tc.temp, err, tc.want)
		}
	}
}

// TestCursorPatternAnchoredInWalker pins that the walker matches
// CursorPattern against whole names only. Unanchored, the pattern would
// rewrite the inner x_cursor_1y and collapse both columns together. (Postgres
// fingerprints ignore FETCH portal names, so this uses a column name.)
func TestCursorPatternAnchoredInWalker(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stderr)
	opts, err := NewOptions(false, `(x)_cursor_[0-9]+(y)`, "")
	if err != nil {
		t.Fatal(err)
	}
	a, err := Normalized("SELECT axx_cursor_1yb FROM t", opts)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Normalized("SELECT axx_cursor_2yb FROM t", opts)
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Errorf("axx_cursor_1yb and axx_cursor_2yb share fingerprint %s; the walker matched part of a name", a)
	}
}
