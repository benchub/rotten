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
	inPG18Squashed := fp(pre + "id IN ($1 /*, ... */)")
	inCastBigint := fp(pre + "id IN (1::bigint, 2::bigint, 3::bigint)")
	inNestedCastInt := fp(pre + "id IN (1::bigint::int)")
	notIn := fp(pre + "id NOT IN (1, 2, 3)")
	inSub := fp(pre + "id IN (SELECT user_id FROM accounts)")
	notInSub := fp(pre + "id NOT IN (SELECT user_id FROM accounts)")

	same := []struct{ want, q string }{
		{in, "id = ANY(ARRAY[1])"},
		{in, "id = ANY(ARRAY[1, 2, 3])"},
		{in, "id = any(array[4, 5, 6, 7, 8, 9, 10])"},
		{in, "id = ANY(ARRAY[$1 /*, ... */])"},
		{in, "id = ANY(ARRAY[$1, $2])"},
		{in, "id = ANY($1)"},
		{in, "id IN (1::int)"},
		{in, "id IN (1::int4)"},
		{in, "id IN (1::int, 2::int)"},
		{in, "id IN ($1::int, $2::int)"},
		{in, "id = ANY(ARRAY[1::int, 2])"},
		{in, "id = ANY(ARRAY[1, 2, 3]::int[])"},
		{inCastBigint, "id = ANY(ARRAY[1, 2, 3]::bigint[])"},
		{fp(pre + "id IN ($1::bigint)"), "id = ANY(ARRAY[$1 /*, ... */]::bigint[])"},
		{fp(pre + "id IN (1::bigint::int, 2::int)"), "id = ANY(ARRAY[1::bigint, 2]::int[])"},
		{fp(pre + "id IN (1::bigint, 2::bigint)"), "id = ANY(ARRAY[1, 2]::bigint[])"},
		{notIn, "id <> ALL(ARRAY[1, 2, 3]::int[])"},
		{notIn, "id <> ALL(ARRAY[1, 2, 3])"},
		{notIn, "id != ALL(ARRAY[$1 /*, ... */])"},
		{inSub, "id = ANY(SELECT user_id FROM accounts)"},
		{fp(pre + "id IN (a.x, 2)"), "id = ANY(ARRAY[a.x, 2])"},
		// PG18 squashes multi-element cast lists to text without element
		// casts, so the collected text can only group with the plain list.
		{in, "id IN ($1 /*, ... */)"},
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
		"id = ANY(ARRAY[[1, 2], [3, 4]])",
		"id = ANY(ARRAY[[1, 2], [3, 4]]::int[])",
		"id = ANY(ARRAY[]::int[])",
		"id < ANY(ARRAY[1, 2, 3]::int[])",
		"id OPERATOR(pg_catalog.=) ANY(ARRAY[1, 2])",
		"id <> ALL(SELECT user_id FROM accounts)",
		"id < ANY(SELECT user_id FROM accounts)",
	}
	for _, q := range distinct {
		got := fp(pre + q)
		for _, other := range []string{in, notIn, inSub, notInSub} {
			if got == other {
				t.Errorf("%s: shares fingerprint %s with an IN form, want distinct", q, got)
			}
		}
	}
	for _, q := range []string{
		"id IN (1::bigint)",
		"id IN (1::bigint, 2::bigint, 3::bigint)",
		"id IN (1, 2::bigint)",
		"id IN (1::bigint::int)",
		"id = ANY(ARRAY[1::bigint]::int[])",
	} {
		got := fp(pre + q)
		if got == in {
			t.Errorf("%s: shares fingerprint %s with uncast IN list, want distinct", q, got)
		}
	}
	if got := fp(pre + "id = ANY(ARRAY[1::bigint]::int[])"); got != inNestedCastInt {
		t.Errorf("cast ANY array got %s, want nested IN cast fingerprint %s", got, inNestedCastInt)
	}
	if inPG18Squashed != in {
		t.Errorf("PG18 squashed IN text got %s, want plain IN fingerprint %s", inPG18Squashed, in)
	}
}
