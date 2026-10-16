package fingerprint

import "testing"

// Postgres 17+ pg_stat_statements shows savepoint names as $1, which the
// parser rejects. Each statement kind still gets one parsed fingerprint.
func TestSavepointPlaceholdersFingerprint(t *testing.T) {
	fp := func(q string) string {
		t.Helper()
		f, err := Normalized(q, Options{})
		if err != nil {
			t.Fatalf("Normalized(%q): %v", q, err)
		}
		return f
	}
	save := fp("SAVEPOINT $1")
	release := fp("RELEASE SAVEPOINT $1")
	rollback := fp("ROLLBACK TO SAVEPOINT $1")
	if save == release || save == rollback || release == rollback {
		t.Fatalf("kinds share a fingerprint: savepoint %s, release %s, rollback to %s", save, release, rollback)
	}
	for q, want := range map[string]string{
		"savepoint   $1":                save,
		"  SAVEPOINT\n\t$1 ;":           save,
		"/* app */ SAVEPOINT $1":        save,
		"release $1":                    release,
		"Release Savepoint $2":          release,
		"rollback to $1":                rollback,
		"ROLLBACK TRANSACTION TO $1":    rollback,
		"rollback work to savepoint $1": rollback,
	} {
		if got := fp(q); got != want {
			t.Errorf("Normalized(%q) = %s, want %s", q, got, want)
		}
	}
	// Named savepoints already parsed; the placeholder form groups with them.
	if got := fp("SAVEPOINT active_record_1"); got != save {
		t.Errorf("named savepoint = %s, want %s", got, save)
	}
	// Representative text for the placeholder forms doesn't fall back.
	if got, err := Query("savepoint   $1"); err != nil || got != "SAVEPOINT $1" {
		t.Errorf("Query = %q, %v; want SAVEPOINT $1", got, err)
	}
	// Other $n shapes still fail to parse.
	for _, q := range []string{"SAVEPOINT $1 $2", "ROLLBACK $1", "RELEASE TO $1", "SAVEPOINT $1; SELECT 1"} {
		if _, err := Normalized(q, Options{}); err != ErrParse {
			t.Errorf("Normalized(%q) err = %v, want ErrParse", q, err)
		}
	}
}
