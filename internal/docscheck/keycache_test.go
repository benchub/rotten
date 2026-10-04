package docscheck

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/auth"
)

// The server's key cache TTL is auth.DefaultTTL and isn't configurable, so
// every doc that says how long a revoked key keeps working must give that
// number and mustn't call it configurable or a default.
func TestKeyCacheTTLDocumented(t *testing.T) {
	root := Root(t)
	setsTTL := regexp.MustCompile(`\.TTL\b|\bTTL\s*:`)
	err := filepath.WalkDir(root, func(path string, e os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if e.IsDir() {
			switch {
			case rel != "." && strings.HasPrefix(e.Name(), "."), rel == "vendor", rel == "ui", rel == "internal/auth":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if m := setsTTL.Find(b); m != nil {
			t.Errorf("%s mentions %q, so the key cache TTL may be configurable; document how to configure it and update this check", rel, m)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if auth.DefaultTTL%time.Second != 0 {
		t.Fatalf("auth.DefaultTTL = %v isn't whole seconds; update this check", auth.DefaultTTL)
	}
	want := fmt.Sprintf("%d seconds", auth.DefaultTTL/time.Second)
	wrong := regexp.MustCompile(`(?i)configurable[^.]*\bTTL\b|\bTTL\b[^.]*\bconfigurable|\bTTL\b[^.]*\bdefault|\d+ seconds by default`)

	for _, path := range []string{
		"docs/plan.md",
		"docs/keys.md",
		"docs/server.md",
		"ui/README.md",
		"ui/app/views/api_keys/index.html.erb",
		"cmd/rotten-server/keys.go",
	} {
		b, err := os.ReadFile(filepath.Join(Root(t), path))
		if err != nil {
			t.Fatal(err)
		}
		// Join wrapped lines so phrases can be matched across them.
		text := strings.Join(strings.Fields(string(b)), " ")
		if !strings.Contains(text, want) {
			t.Errorf("%s doesn't give the key cache TTL as %q", path, want)
		}
		if m := wrong.FindString(text); m != "" {
			t.Errorf("%s says %q, but the key cache TTL is fixed", path, m)
		}
	}
}
