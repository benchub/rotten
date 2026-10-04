package pgss

import (
	"strings"
	"testing"
)

func TestCheckSupportedVersion(t *testing.T) {
	for _, c := range []struct {
		s    string
		want [2]int
	}{
		{"1.9", [2]int{1, 9}},
		{"1.11", [2]int{1, 11}},
		{"1.11.1", [2]int{1, 11}},
	} {
		got, err := checkSupportedVersion(c.s)
		if err != nil || got != c.want {
			t.Errorf("checkSupportedVersion(%q) = %v, %v; want %v", c.s, got, err, c.want)
		}
	}
	for _, c := range []struct{ s, msg string }{
		{"2.0", "pg_stat_statements 2.0 has unsupported major version 2; rotten supports 1.x"},
		{"2", "pg_stat_statements 2 has unsupported major version 2; rotten supports 1.x"},
		{"0.9", "pg_stat_statements 0.9 has unsupported major version 0; rotten supports 1.x"},
		{"1.8", "extversion 1.8 is older than 1.9 (Postgres 14)"},
		{"1.11beta", `unexpected extversion "1.11beta"`},
	} {
		_, err := checkSupportedVersion(c.s)
		if err == nil || !strings.Contains(err.Error(), c.msg) {
			t.Errorf("checkSupportedVersion(%q): err = %v, want %q", c.s, err, c.msg)
		}
	}
}

func TestHasMinmaxResetVersion(t *testing.T) {
	for _, c := range []struct {
		s    string
		want bool
	}{
		{"2.0", true},
		{"2", true},
		{"1.11", true},
		{"1.12", true},
		{"1.10", false},
		{"1.9", false},
	} {
		v, err := parseExtVersion(c.s)
		if err != nil {
			t.Errorf("parseExtVersion(%q): %v", c.s, err)
			continue
		}
		if got := hasMinmaxReset(v); got != c.want {
			t.Errorf("hasMinmaxReset(%q) = %v, want %v", c.s, got, c.want)
		}
	}
}

func TestParseExtVersion(t *testing.T) {
	for _, c := range []struct {
		s    string
		want [2]int
	}{
		{"1.9", [2]int{1, 9}},
		{"1.11", [2]int{1, 11}},
		{"2", [2]int{2, 0}},
		{"2.0", [2]int{2, 0}},
		{"1.11.1", [2]int{1, 11}},
	} {
		got, err := parseExtVersion(c.s)
		if err != nil || got != c.want {
			t.Errorf("parseExtVersion(%q) = %v, %v; want %v", c.s, got, err, c.want)
		}
	}
	// observer.sql's int[] cast fails on these, so the Go side errors too.
	for _, s := range []string{"", "1.11beta", "1.", ".11", "a.b", "1.11.1x", "-1.11", "1.-2"} {
		if v, err := parseExtVersion(s); err == nil {
			t.Errorf("parseExtVersion(%q) = %v, want error", s, v)
		}
	}
}
