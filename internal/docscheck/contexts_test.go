package docscheck

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// docSubsection returns the body of the "### title" section of doc, up to
// the next heading of level 3 or higher.
func docSubsection(t *testing.T, doc, title string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(Root(t), doc))
	if err != nil {
		t.Fatal(err)
	}
	_, rest, ok := strings.Cut(string(b), "\n### "+title+"\n")
	if !ok {
		t.Fatalf("%s has no %q section", doc, "### "+title)
	}
	if loc := regexp.MustCompile(`(?m)^#{1,3} `).FindStringIndex(rest); loc != nil {
		rest = rest[:loc[0]]
	}
	return rest
}

// pg_stat_statements keeps one text per entry, the first it saw, and the
// worker credits all the entry's calls to the context in that text. The
// docs that describe contexts must say so, and say what a count means.
func TestContextCreditingDocumented(t *testing.T) {
	flat := func(s string) string { return strings.Join(strings.Fields(s), " ") }

	worker := flat(docSubsection(t, "docs/worker.md", "Query context"))
	for _, want := range []string{
		"`pg_stat_statements` keeps one query text for each entry",
		"first seen under that context",
	} {
		if !strings.Contains(worker, want) {
			t.Errorf("docs/worker.md, Query context: missing %q", want)
		}
	}

	b, err := os.ReadFile(filepath.Join(Root(t), "ui/README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(flat(string(b)), "first seen under that context") {
		t.Errorf("ui/README.md: missing the context caveat (%q)", "first seen under that context")
	}
}
