package fingerprint

import (
	"io"
	"log"
	"os"
	"testing"
)

// TestAnyArrayGroupsWithIn checks which ANY/ALL forms share a fingerprint
// with the IN-list form and which must stay distinct. The golden file pins
// the values; this test pins the grouping.
func TestAnyArrayGroupsWithIn(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stderr)

	fp := func(q string) string {
		t.Helper()
		f, err := Normalized(q, Options{})
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return f
	}
	const pre = "SELECT * FROM users WHERE "
	in := fp(pre + "id IN (1, 2, 3)")
	notIn := fp(pre + "id NOT IN (1, 2, 3)")
	inSub := fp(pre + "id IN (SELECT user_id FROM accounts)")

	same := []struct{ want, q string }{
		{in, "id = ANY(ARRAY[1])"},
		{in, "id = ANY(ARRAY[1, 2, 3])"},
		{in, "id = any(array[4, 5, 6, 7, 8, 9, 10])"},
		{in, "id = ANY(ARRAY[$1 /*, ... */])"},
		{in, "id = ANY(ARRAY[$1, $2])"},
		{in, "id = ANY($1)"},
		{in, "id = ANY(ARRAY[1::int, 2])"},
		{notIn, "id <> ALL(ARRAY[1, 2, 3])"},
		{notIn, "id != ALL(ARRAY[$1 /*, ... */])"},
		{inSub, "id = ANY(SELECT user_id FROM accounts)"},
		{fp(pre + "id IN (a.x, 2)"), "id = ANY(ARRAY[a.x, 2])"},
	}
	for _, c := range same {
		if got := fp(pre + c.q); got != c.want {
			t.Errorf("%s: got %s, want the IN form's %s", c.q, got, c.want)
		}
	}

	distinct := []string{
		"id < ANY(ARRAY[1, 2, 3])",
		"id = ALL(ARRAY[1, 2, 3])",
		"id <> ANY(ARRAY[1, 2, 3])",
		"id = ANY(ARRAY[1, 2, 3]::bigint[])",
		"id = ANY(ARRAY[[1, 2], [3, 4]])",
		"id OPERATOR(pg_catalog.=) ANY(ARRAY[1, 2])",
		"id <> ALL(SELECT user_id FROM accounts)",
		"id < ANY(SELECT user_id FROM accounts)",
	}
	for _, q := range distinct {
		got := fp(pre + q)
		for _, other := range []string{in, notIn, inSub} {
			if got == other {
				t.Errorf("%s: shares fingerprint %s with an IN form, want distinct", q, got)
			}
		}
	}
}
