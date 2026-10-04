package docscheck

import (
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// rubyConst returns the value of NAME = value in the Ruby file at path
// (relative to the repo root), failing t if it isn't defined exactly once.
func rubyConst(t *testing.T, path, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(Root(t), path))
	if err != nil {
		t.Fatal(err)
	}
	re := regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(name) + `\s*=\s*(\S+)\s*$`)
	m := re.FindAllStringSubmatch(string(b), -1)
	if len(m) != 1 {
		t.Fatalf("%s: want one definition of %s, found %d", path, name, len(m))
	}
	return m[0][1]
}

// docSection returns the body of the "## title" section of doc, as written.
func docSection(t *testing.T, doc, title string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(Root(t), doc))
	if err != nil {
		t.Fatal(err)
	}
	_, rest, ok := strings.Cut(string(b), "\n## "+title+"\n")
	if !ok {
		t.Fatalf("%s has no %q section", doc, "## "+title)
	}
	if i := strings.Index(rest, "\n## "); i >= 0 {
		rest = rest[:i]
	}
	return rest
}

// tableRow returns the one Markdown table row in section that mentions key.
func tableRow(t *testing.T, section, key string) string {
	t.Helper()
	var rows []string
	for _, line := range strings.Split(section, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "|") && strings.Contains(line, key) {
			rows = append(rows, line)
		}
	}
	if len(rows) != 1 {
		t.Fatalf("want one table row mentioning %s, found %d", key, len(rows))
	}
	return rows[0]
}

var limitNumber = regexp.MustCompile(`(\d+) (attempts per \w+|minutes)\b`)

// rowLimits maps each "N attempts per X" and "N minutes" in row to its
// numbers, so a row can't state a limit twice with different values.
func rowLimits(row string) map[string][]string {
	got := make(map[string][]string)
	for _, m := range limitNumber.FindAllStringSubmatch(row, -1) {
		got[m[2]] = append(got[m[2]], m[1])
	}
	return got
}

// The login and password-change rate limits count in a memory store in each
// process, so docs/ui.md must say so and give the limits the code sets.
func TestUIRateLimitsDocumented(t *testing.T) {
	const sessions = "ui/app/controllers/sessions_controller.rb"
	const passwords = "ui/app/controllers/passwords_controller.rb"

	perIP := rubyConst(t, sessions, "ATTEMPTS_PER_IP")
	perEmail := rubyConst(t, sessions, "ATTEMPTS_PER_EMAIL")
	window := rubyConst(t, sessions, "ATTEMPTS_WINDOW")
	minutes, ok := strings.CutSuffix(window, ".minutes")
	if !ok {
		t.Fatalf("%s: ATTEMPTS_WINDOW = %s; expected N.minutes, so update this check", sessions, window)
	}
	// The password change limits reuse login's. If they get their own
	// numbers, this check and the doc need them too.
	for name, want := range map[string]string{
		"ATTEMPTS_PER_IP":   "SessionsController::ATTEMPTS_PER_IP",
		"ATTEMPTS_PER_USER": "SessionsController::ATTEMPTS_PER_EMAIL",
		"ATTEMPTS_WINDOW":   "SessionsController::ATTEMPTS_WINDOW",
	} {
		if got := rubyConst(t, passwords, name); got != want {
			t.Errorf("%s: %s = %s, want %s; document the new limit and update this check", passwords, name, got, want)
		}
	}

	raw := docSection(t, "docs/ui.md", "Login rate limits")
	for key, want := range map[string]map[string][]string{
		"`POST /login`": {
			"attempts per client": {perIP},
			"attempts per email":  {perEmail},
			"minutes":             {minutes},
		},
		"`PATCH /password`": {
			"attempts per client": {perIP},
			"attempts per user":   {perEmail},
			"minutes":             {minutes},
		},
	} {
		row := tableRow(t, raw, key)
		if got := rowLimits(row); !maps.EqualFunc(got, want, slices.Equal) {
			t.Errorf("docs/ui.md %s row states limits %v, want %v:\n%s", key, got, want, row)
		}
	}

	// Join wrapped lines so phrases can be matched across them.
	section := strings.Join(strings.Fields(raw), " ")
	for _, phrase := range []string{
		"each process keeps its own counters",
		"N processes allow N times",
		"run one UI process",
		"scale the limits down",
		"at least 1",
		"dividing `ATTEMPTS_PER_IP` and `ATTEMPTS_PER_EMAIL`",
		"Leave `ATTEMPTS_WINDOW` unchanged",
		"`WEB_CONCURRENCY`",
	} {
		if !strings.Contains(section, phrase) {
			t.Errorf("docs/ui.md \"Login rate limits\" section doesn't say %q", phrase)
		}
	}
}
