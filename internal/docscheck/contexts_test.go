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

// Context counts are exact since task 20261007-120000-6, so ui/README.md
// drops the first-seen caveat and says how untagged calls show.
func TestContextCaveatGoneFromUIReadme(t *testing.T) {
	flat := func(s string) string { return strings.Join(strings.Fields(s), " ") }
	b, err := os.ReadFile(filepath.Join(Root(t), "ui/README.md"))
	if err != nil {
		t.Fatal(err)
	}
	doc := flat(string(b))
	for _, gone := range []string{"first seen under that context", "Contexts are approximate", "context-caveat"} {
		if strings.Contains(doc, gone) {
			t.Errorf("ui/README.md: still says %q", gone)
		}
	}
	for _, want := range []string{"untagged", "pg_stat_statement_context"} {
		if !strings.Contains(doc, want) {
			t.Errorf("ui/README.md: missing %q", want)
		}
	}
}

// The Postgres 18 advice to append marginalia is gone: pssc reads
// prepended comments. docs/observed.md says how to install pssc, that it's
// optional, and which settings marginalia needs.
func TestObservedDocsUsePssc(t *testing.T) {
	flat := func(s string) string { return strings.Join(strings.Fields(s), " ") }
	read := func(doc string) string {
		b, err := os.ReadFile(filepath.Join(Root(t), doc))
		if err != nil {
			t.Fatal(err)
		}
		return flat(string(b))
	}
	for _, doc := range []string{"docs/observed.md", "docs/worker.md", "dev/README.md", "ui/README.md"} {
		text := read(doc)
		for _, gone := range []string{"prepend_comment", "appended, not prepended", "drops a leading comment",
			"credits its calls to its first context"} {
			if strings.Contains(text, gone) {
				t.Errorf("%s: still says %q", doc, gone)
			}
		}
	}
	observed := read("docs/observed.md")
	for _, want := range []string{
		"pg_stat_statement_context",
		"optional",
		"shared_preload_libraries = 'pg_stat_statements,pg_stat_statement_context'",
		"CREATE EXTENSION pg_stat_statement_context;",
		"pg_stat_statement_context.extractors",
		"position=any",
		"position=prepend",
		"pg_stat_statement_context.tags",
		"job_tag",
		"untagged",
		"RDS",
	} {
		if !strings.Contains(observed, want) {
			t.Errorf("docs/observed.md: missing %q", want)
		}
	}
}
