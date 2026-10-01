package main

import (
	"bytes"
	"strings"
	"testing"
)

// An invalid retention fails before migrate connects, so the bogus DSN is
// never dialed.
func TestMigrateRejectsInvalidRetention(t *testing.T) {
	for _, args := range [][]string{
		{"migrate", "-dsn", "postgres://nowhere.invalid/x", "-retention", "0d"},
		{"migrate", "-dsn", "postgres://nowhere.invalid/x", "-retention", "21 days; drop table x"},
	} {
		var out, errb bytes.Buffer
		if code := run(args, &out, &errb); code != 2 {
			t.Errorf("run(%q) = %d, want 2; stderr %q", args, code, errb.String())
		}
		if !strings.Contains(errb.String(), "retention") {
			t.Errorf("run(%q) stderr = %q, want a retention error", args, errb.String())
		}
	}
}

// The flag wins over ROTTEN_RETENTION: a bad flag fails even with a good
// env value, and a good flag gets past a bad env value to the DSN check.
func TestMigrateRetentionFlagOverridesEnv(t *testing.T) {
	t.Setenv("ROTTEN_RETENTION", "30d")
	var out, errb bytes.Buffer
	if code := run([]string{"migrate", "-dsn", "postgres://nowhere.invalid/x", "-retention", "bad"}, &out, &errb); code != 2 ||
		!strings.Contains(errb.String(), `retention "bad"`) {
		t.Errorf("good env, bad flag: code %d, stderr %q; want 2 and the flag's value", code, errb.String())
	}

	t.Setenv("ROTTEN_RETENTION", "bad")
	t.Setenv("ROTTEN_OWNER_DSN", "")
	out.Reset()
	errb.Reset()
	if code := run([]string{"migrate", "-retention", "30d"}, &out, &errb); code != 2 ||
		strings.Contains(errb.String(), "retention") || !strings.Contains(errb.String(), "no DSN") {
		t.Errorf("bad env, good flag: code %d, stderr %q; want the DSN error, not a retention one", code, errb.String())
	}
}

func TestMigrateRejectsExtraArgs(t *testing.T) {
	var out, errb bytes.Buffer
	code := run([]string{"migrate", "-dsn", "postgres://nowhere.invalid/x", "extra"}, &out, &errb)
	if code != 2 || !strings.Contains(errb.String(), "unexpected arguments") {
		t.Errorf("code %d, stderr %q; want 2 and an unexpected-arguments error", code, errb.String())
	}
}

func TestMigrateRetentionFromEnv(t *testing.T) {
	t.Setenv("ROTTEN_RETENTION", "nope")
	var out, errb bytes.Buffer
	if code := run([]string{"migrate", "-dsn", "postgres://nowhere.invalid/x"}, &out, &errb); code != 2 {
		t.Errorf("run = %d, want 2; stderr %q", code, errb.String())
	}
	if !strings.Contains(errb.String(), `retention "nope"`) {
		t.Errorf("stderr = %q, want it to name ROTTEN_RETENTION's value", errb.String())
	}
}
