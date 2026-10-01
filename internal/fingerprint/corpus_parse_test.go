package fingerprint

import (
	"strings"
	"testing"
)

func TestParseCorpusSkipsCommentLines(t *testing.T) {
	in := `-- file comment
-- case: first
SELECT 1 -- trailing
  /* inline */ FROM t
-- a comment between cases
-- case: second
SELECT 2
`
	got := parseCorpus(t, strings.NewReader(in))
	want := []corpusCase{
		{name: "first", query: "SELECT 1 -- trailing\n  /* inline */ FROM t"},
		{name: "second", query: "SELECT 2"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d cases, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("case %d: got %#v, want %#v", i, got[i], want[i])
		}
	}
}
