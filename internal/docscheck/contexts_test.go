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

// Contexts come from pg_stat_statement_context, which is optional, and it
// needs two settings changed for marginalia. docs/worker.md must say so,
// and that the old context keys are gone.
func TestWorkerContextsDocumented(t *testing.T) {
	flat := func(s string) string { return strings.Join(strings.Fields(s), " ") }

	worker := flat(docSubsection(t, "docs/worker.md", "Query context"))
	for _, want := range []string{
		"pg_stat_statement_context",
		"optional",
		"untagged",
		"position=any",
		"job_tag",
		"utility_missing_queryid",
	} {
		if !strings.Contains(worker, want) {
			t.Errorf("docs/worker.md, Query context: missing %q", want)
		}
	}
	for _, gone := range []string{"first seen under that context", "No marginalia contexts found"} {
		if strings.Contains(worker, gone) {
			t.Errorf("docs/worker.md, Query context: still says %q", gone)
		}
	}
	removed := flat(docSubsection(t, "docs/worker.md", "Removed keys"))
	for _, key := range []string{"`ContextController`", "`ContextAction`", "`ContextJob`"} {
		if !strings.Contains(removed, key) {
			t.Errorf("docs/worker.md, Removed keys: missing %s", key)
		}
	}
}

// The UI still shows the first-seen caveat until task 20261007-120000-6.
func TestContextCaveatInUIReadme(t *testing.T) {
	flat := func(s string) string { return strings.Join(strings.Fields(s), " ") }
	b, err := os.ReadFile(filepath.Join(Root(t), "ui/README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(flat(string(b)), "first seen under that context") {
		t.Errorf("ui/README.md: missing the context caveat (%q)", "first seen under that context")
	}
}
