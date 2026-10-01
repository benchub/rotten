package fingerprint

import (
	"io"
	"log"
	"os"
	"testing"
)

// TestSchemaCollapsing covers both schema modes. The golden file pins the
// default mode. This test also pins that KeepSchemas turns collapsing off.
func TestSchemaCollapsing(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stderr)

	queries := []string{
		"SELECT * FROM users WHERE id = 1",
		"SELECT * FROM public.users WHERE id = 2",
		"SELECT * FROM shard_1.users WHERE id = 3",
		"SELECT * FROM shard_27.users WHERE id = 4",
	}
	fps := func(t *testing.T, opts Options) []string {
		var out []string
		for _, q := range queries {
			fp, err := Normalized(q, opts)
			if err != nil {
				t.Fatalf("Normalized(%q, %+v): %v", q, opts, err)
			}
			out = append(out, fp)
		}
		return out
	}

	t.Run("default collapses", func(t *testing.T) {
		got := fps(t, Options{})
		for i, fp := range got {
			if fp != got[0] {
				t.Errorf("%q = %s, want %s (same as %q)", queries[i], fp, got[0], queries[0])
			}
		}
	})

	t.Run("KeepSchemas keeps them apart", func(t *testing.T) {
		got := fps(t, Options{KeepSchemas: true})
		seen := map[string]string{}
		for i, fp := range got {
			if prev, ok := seen[fp]; ok {
				t.Errorf("%q and %q share fingerprint %s", prev, queries[i], fp)
			}
			seen[fp] = queries[i]
		}
	})

	t.Run("CREATE SCHEMA", func(t *testing.T) {
		for _, tc := range []struct {
			opts Options
			same bool
		}{{Options{}, true}, {Options{KeepSchemas: true}, false}} {
			a, err := Normalized("CREATE SCHEMA shard_1", tc.opts)
			if err != nil {
				t.Fatal(err)
			}
			b, err := Normalized("CREATE SCHEMA shard_27", tc.opts)
			if err != nil {
				t.Fatal(err)
			}
			if (a == b) != tc.same {
				t.Errorf("%+v: shard_1 = %s, shard_27 = %s, want same=%v", tc.opts, a, b, tc.same)
			}
		}
	})

	t.Run("KeepSchemas still collapses literals", func(t *testing.T) {
		a, err := Normalized("SELECT * FROM shard_1.users WHERE id = 1", Options{KeepSchemas: true})
		if err != nil {
			t.Fatal(err)
		}
		b, err := Normalized("SELECT * FROM shard_1.users WHERE id = 99", Options{KeepSchemas: true})
		if err != nil {
			t.Fatal(err)
		}
		if a != b {
			t.Errorf("literals split fingerprints: %s vs %s", a, b)
		}
	})
}
