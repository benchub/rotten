// Package docscheck backs the docs smoke tests: every config key, flag and
// environment variable that rotten reads must be mentioned in the operator
// doc that owns it, such as docs/server.md for the server's settings.
//
// A name counts as documented when it appears as a token inside a Markdown
// code span or fenced code block of that doc, so a key like Role isn't
// satisfied by the word "role" in prose, and a mention in some other doc
// doesn't count.
package docscheck

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Root returns the repository root, the nearest parent holding go.mod.
func Root(t testing.TB) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("docscheck: no go.mod above the working directory")
		}
		dir = parent
	}
}

var (
	codeSpan = regexp.MustCompile("`+([^`]+)`+")
	tokenSep = regexp.MustCompile(`[^A-Za-z0-9_.\-]+`)
)

// CodeTokens returns every token that appears in a code span or fenced code
// block of doc, a Markdown file relative to the repo root. Tokens are split
// on anything other than letters, digits, '_', '.' and '-'. Each token is
// kept as is and also with leading and trailing dots and dashes trimmed, so
// `-dsn` yields both "-dsn" and "dsn".
func CodeTokens(t testing.TB, doc string) map[string]bool {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(Root(t), doc))
	if err != nil {
		t.Fatalf("docscheck: %v", err)
	}
	tokens := make(map[string]bool)
	add := func(code string) {
		for _, tok := range tokenSep.Split(code, -1) {
			if tok == "" {
				continue
			}
			tokens[tok] = true
			if trimmed := strings.Trim(tok, ".-"); trimmed != "" {
				tokens[trimmed] = true
			}
		}
	}
	inFence := false
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			inFence = !inFence
			continue
		}
		if inFence {
			add(line)
			continue
		}
		for _, m := range codeSpan.FindAllStringSubmatch(line, -1) {
			add(m[1])
		}
	}
	return tokens
}

// Missing returns the names that aren't code tokens in doc, sorted.
func Missing(t testing.TB, doc string, names []string) []string {
	t.Helper()
	tokens := CodeTokens(t, doc)
	var missing []string
	for _, n := range names {
		if !tokens[n] {
			missing = append(missing, n)
		}
	}
	sort.Strings(missing)
	return missing
}

// MissingFlags is Missing for command-line flags: each must appear as -name
// or --name. The missing ones are returned as -name.
func MissingFlags(t testing.TB, doc string, names []string) []string {
	t.Helper()
	tokens := CodeTokens(t, doc)
	var missing []string
	for _, n := range names {
		if !tokens["-"+n] && !tokens["--"+n] {
			missing = append(missing, "-"+n)
		}
	}
	sort.Strings(missing)
	return missing
}

// RequireDocumented fails t, listing every name that isn't a code token in
// doc. what describes the names in the failure message.
func RequireDocumented(t testing.TB, doc, what string, names []string) {
	t.Helper()
	if len(names) == 0 {
		t.Fatalf("docscheck: found no %s to check; the extraction is broken", what)
	}
	if missing := Missing(t, doc, names); len(missing) > 0 {
		t.Errorf("%s not mentioned in a code span in %s:\n  %s", what, doc, strings.Join(missing, "\n  "))
	}
}

// RequireFlagsDocumented is RequireDocumented for command-line flags.
func RequireFlagsDocumented(t testing.TB, doc, what string, names []string) {
	t.Helper()
	if len(names) == 0 {
		t.Fatalf("docscheck: found no %s to check; the extraction is broken", what)
	}
	if missing := MissingFlags(t, doc, names); len(missing) > 0 {
		t.Errorf("%s not mentioned in a code span in %s:\n  %s", what, doc, strings.Join(missing, "\n  "))
	}
}

var flagUsageLine = regexp.MustCompile(`(?m)^\s+-([A-Za-z0-9][A-Za-z0-9_.\-]*)`)

// FlagNames extracts flag names from a flag package usage message, the
// "  -name type" lines PrintDefaults writes.
func FlagNames(usage string) []string {
	seen := make(map[string]bool)
	for _, m := range flagUsageLine.FindAllStringSubmatch(usage, -1) {
		seen[m[1]] = true
	}
	return sortedKeys(seen)
}

// GoStringLiterals returns the distinct string literals in the non-test Go
// files matching glob (relative to the repo root) that fully match re.
func GoStringLiterals(t testing.TB, glob string, re *regexp.Regexp) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(Root(t), glob))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) == 0 {
		t.Fatalf("docscheck: no Go files match %s", glob)
	}
	seen := make(map[string]bool)
	fset := token.NewFileSet()
	for _, path := range matches {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("docscheck: %v", err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			s, err := strconv.Unquote(lit.Value)
			if err == nil && re.MatchString(s) {
				seen[s] = true
			}
			return true
		})
	}
	return sortedKeys(seen)
}

var (
	// Process env reads with a literal name: ENV["X"], ENV.fetch("X"),
	// ENV.key?("X").
	rubyENV = regexp.MustCompile(`\bENV\s*(?:\[|\.fetch\(|\.key\?\(|\.has_key\?\()\s*["']([A-Z][A-Z0-9_]*)["']`)
	// A method that takes the env hash as a parameter defaulting to ENV, so
	// it can be tested with a fake env: def self.fetch!(env = ENV). Call
	// sites that pass env: ENV don't count; the hash there may be Rack's.
	rubyEnvParam = regexp.MustCompile(`(?m)^\s*def\b[^\n]*\benv\s*(?:=|:)\s*ENV\b`)
	// In such files, env var names are whole all-caps string literals or %w
	// words with at least one underscore, like "OIDC_ISSUER".
	rubyQuotedName = regexp.MustCompile(`["']([A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+)["']`)
	rubyWordArray  = regexp.MustCompile(`%w[\[(]([^\])]*)[\])]`)
	rubyEnvName    = regexp.MustCompile(`^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+$`)
)

// RubyEnvNames returns the environment variable names that the Ruby, ERB and
// YAML files under dirs (relative to the repo root) read. Comment lines are
// skipped. It finds direct ENV reads everywhere, and, in files whose methods
// take an env hash defaulting to ENV, every all-caps name in a string literal
// or %w array.
func RubyEnvNames(t testing.TB, dirs ...string) []string {
	t.Helper()
	root := Root(t)
	seen := make(map[string]bool)
	for _, d := range dirs {
		err := filepath.WalkDir(filepath.Join(root, d), func(path string, e os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if e.IsDir() {
				return nil
			}
			switch filepath.Ext(path) {
			case ".rb", ".erb", ".yml", ".yaml", ".rake":
			default:
				return nil
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			var code []string
			for _, line := range strings.Split(string(b), "\n") {
				if strings.HasPrefix(strings.TrimSpace(line), "#") {
					continue
				}
				code = append(code, line)
			}
			src := strings.Join(code, "\n")
			for _, m := range rubyENV.FindAllStringSubmatch(src, -1) {
				seen[m[1]] = true
			}
			if rubyEnvParam.MatchString(src) {
				for _, m := range rubyQuotedName.FindAllStringSubmatch(src, -1) {
					seen[m[1]] = true
				}
				for _, m := range rubyWordArray.FindAllStringSubmatch(src, -1) {
					for _, w := range strings.Fields(m[1]) {
						if rubyEnvName.MatchString(w) {
							seen[w] = true
						}
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("docscheck: %v", err)
		}
	}
	return sortedKeys(seen)
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
