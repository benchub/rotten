package docscheck

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const contextSamplingDoc = "docs/decisions/context-sampling.md"

// Task 20261004-150000-1: the design for per-context attribution by
// sampling pg_stat_activity must exist and cover each point the backlog
// task and its review asked for, before anything is built.
func TestContextSamplingDesignCoversTheTask(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(Root(t), contextSamplingDoc))
	if err != nil {
		t.Fatalf("the design doc is missing: %v", err)
	}
	doc := string(b)
	flat := strings.ToLower(strings.Join(strings.Fields(doc), " "))

	sections := level2Sections(doc)
	// Superseded by pg_stat_statement_context (task 20261007-120000-6).
	status := strings.Join(strings.Fields(sections["Status."]), " ")
	for _, want := range []string{"Superseded", "pg_stat_statement_context"} {
		if !strings.Contains(status, want) {
			t.Errorf("%s: the Status section doesn't say %q", contextSamplingDoc, want)
		}
	}
	if strings.Contains(status, "the UI caveat stays") {
		t.Errorf("%s: the Status section still says the UI caveat stays", contextSamplingDoc)
	}
	for _, h := range []string{
		"Status.",
		"The problem.",
		"What sampling can and can't estimate.",
		"Sampling pg_stat_activity.",
		"Estimating each context's share.",
		"How the estimates are stored.",
		"Bias against short queries.",
		"Sampling rate and cost.",
		"Truncation by track_activity_query_size.",
		"Postgres 18 and leading comments.",
		"Replicas.",
		"Sample lifecycle.",
		"Harvest size limits.",
		"Statistical error.",
		"Showing estimates honestly in the UI.",
		"Fallback when there are no samples.",
		"Config changes.",
		"Proto changes.",
		"Validation plan.",
		"Alternatives considered.",
		"Is this worth building?",
		"Recommendation.",
		"Open questions for sign-off.",
		"Proposed task breakdown.",
	} {
		body, ok := sections[h]
		if !ok {
			t.Errorf("%s: missing the section %q", contextSamplingDoc, "## "+h)
			continue
		}
		if strings.TrimSpace(body) == "" {
			t.Errorf("%s: the section %q is empty", contextSamplingDoc, "## "+h)
		}
	}

	// Each point the task and the review name, by a phrase the doc must use.
	for _, want := range []string{
		"pg_stat_activity",
		"query_id",
		"compute_query_id",
		"auto",
		"utility statements",
		"pg_read_all_stats",
		"state = 'idle'",
		"track_activity_query_size",
		"trailing comment",
		"octet_length",
		"first-text attribution",
		"minimum samples",
		"confidence",
		"activity share",
		"steady mix",
		"low data",
		"sightings",
		"boundary executions",
		"planning",
		"poisson",
		"unattributed",
		"multi-statement",
		"set role",
		"node identity",
		"pg_postmaster_start_time",
		"pgss.recreated",
		"maxharvestcontexts",
		"log_min_duration_statement",
		"log_statement_sample_rate",
		"auto_explain",
		"pg_stat_monitor",
		"app-side",
		"opt-in",
		"sign-off",
	} {
		if !strings.Contains(flat, want) {
			t.Errorf("%s: missing %q", contextSamplingDoc, want)
		}
	}

	// The proposed tasks use the backlog's ID form and say what they need,
	// so the main session can paste them in after sign-off.
	tasks := regexp.MustCompile(`(?m)^### `).Split(sections["Proposed task breakdown."], -1)[1:]
	if len(tasks) < 3 {
		t.Errorf("%s: the proposed task breakdown has %d tasks; split the build into at least 3", contextSamplingDoc, len(tasks))
	}
	taskHeading := regexp.MustCompile(`^\d{8}-\d{6}-\d+: \S`)
	for _, task := range tasks {
		heading, _, _ := strings.Cut(task, "\n")
		if !taskHeading.MatchString(heading) {
			t.Errorf("%s: task %q doesn't start with a YYYYMMDD-HHMMSS-N ID", contextSamplingDoc, heading)
		}
		if !strings.Contains(task, "\n- **Needs:** ") {
			t.Errorf("%s: task %q has no Needs line", contextSamplingDoc, heading)
		}
	}
}

// level2Sections maps each "## title" in doc to its body, up to the next
// level 1 or level 2 heading. Fenced code blocks are skipped when looking
// for headings.
func level2Sections(doc string) map[string]string {
	sections := make(map[string]string)
	var title string
	var body strings.Builder
	inFence, inSection := false, false
	flush := func() {
		if inSection {
			sections[title] = body.String()
		}
		body.Reset()
	}
	for _, line := range strings.Split(doc, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			inFence = !inFence
		}
		if !inFence && (strings.HasPrefix(line, "## ") || strings.HasPrefix(line, "# ")) {
			flush()
			inSection = strings.HasPrefix(line, "## ")
			title = strings.TrimSpace(strings.TrimPrefix(line, "## "))
			continue
		}
		body.WriteString(line + "\n")
	}
	flush()
	return sections
}
