package devtraffic_test

import (
	"encoding/json"
	"math/rand/v2"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/benchub/rotten/internal/devtraffic"
	"github.com/benchub/rotten/internal/fingerprint"
	"github.com/benchub/rotten/internal/identity"
	"github.com/benchub/rotten/internal/testdb"
)

// devWorkerConfig is the part of dev/worker.json these tests need.
type devWorkerConfig struct {
	ContextController string
	ContextAction     string
	ContextJob        string
	KeepSchemas       bool
	CursorPattern     string
	TempTablePattern  string
}

func loadDevWorkerConfig(t *testing.T) devWorkerConfig {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testdb.RepoRoot(), "dev", "worker.json"))
	if err != nil {
		t.Fatal(err)
	}
	var cfg devWorkerConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}

// devRegexes compiles dev/worker.json's context regexes with the worker's
// own compile step.
func devRegexes(t *testing.T) (c, a, j *regexp.Regexp) {
	t.Helper()
	cfg := loadDevWorkerConfig(t)
	c, a, j, err := identity.CompileRegexes(cfg.ContextController, cfg.ContextAction, cfg.ContextJob)
	if err != nil {
		t.Fatal(err)
	}
	return c, a, j
}

func devFingerprintOptions(t *testing.T) fingerprint.Options {
	t.Helper()
	cfg := loadDevWorkerConfig(t)
	opts, err := fingerprint.NewOptions(cfg.KeepSchemas, cfg.CursorPattern, cfg.TempTablePattern)
	if err != nil {
		t.Fatal(err)
	}
	return opts
}

// lastGroup is how identity.Cache.Find and the worker read a context value:
// the last capture group of the first match, or "" when there's none. The
// real-Postgres test runs the worker itself, so its extraction is covered
// there too.
func lastGroup(re *regexp.Regexp, query string) string {
	m := re.FindStringSubmatch(query)
	if len(m) <= 1 {
		return ""
	}
	return m[len(m)-1]
}

var (
	uuidRE       = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	jobContextRE = regexp.MustCompile(`^[1-9][0-9]{11}$`)
	webHostRE    = regexp.MustCompile(`^app0100012202[0-9]{2}$`)
	jobHostRE    = regexp.MustCompile(`^job0100010452[0-9]{2}$`)
	pidRE        = regexp.MustCompile(`^[1-9][0-9]*$`)
	leadingRE    = regexp.MustCompile(`^/\*([^*]*)\*/ [^/]+$`)
	trailingRE   = regexp.MustCompile(`^[^/]+ /\*([^*]*)\*/$`)
	positions    = []devtraffic.Position{devtraffic.Leading, devtraffic.Trailing}
)

// commentFields parses the statement's one marginalia comment, which must be
// at pos, into its keys, in order, and their values.
func commentFields(t *testing.T, stmt string, pos devtraffic.Position) ([]string, map[string]string) {
	t.Helper()
	re := leadingRE
	if pos == devtraffic.Trailing {
		re = trailingRE
	}
	m := re.FindStringSubmatch(stmt)
	if m == nil {
		t.Fatalf("statement has no %s marginalia comment: %q", pos, stmt)
	}
	var keys []string
	vals := map[string]string{}
	for _, kv := range strings.Split(m[1], ",") {
		k, v, ok := strings.Cut(kv, ":")
		if !ok {
			t.Fatalf("comment field %q has no colon in %q", kv, stmt)
		}
		keys = append(keys, k)
		vals[k] = v
	}
	return keys, vals
}

func TestCatalogShapeCountAndUse(t *testing.T) {
	shapes := devtraffic.Shapes()
	if n := len(shapes); n < 20 || n > 30 {
		t.Fatalf("%d query shapes, want 20 to 30", n)
	}
	used := map[string]int{}
	for _, c := range devtraffic.Contexts() {
		if len(c.Shapes) == 0 {
			t.Errorf("context %s runs no shapes", c.Label())
		}
		for _, name := range c.Shapes {
			if _, ok := devtraffic.ShapeByName(name); !ok {
				t.Errorf("context %s names unknown shape %q", c.Label(), name)
			}
			used[name]++
		}
	}
	shared := 0
	for _, s := range shapes {
		switch used[s.Name] {
		case 0:
			t.Errorf("shape %s is not run by any context", s.Name)
		case 1:
		default:
			shared++
		}
	}
	if shared < len(shapes)/2 {
		t.Errorf("only %d of %d shapes run under more than one context; want at least half", shared, len(shapes))
	}
	var web, jobs int
	for _, c := range devtraffic.Contexts() {
		if c.IsJob() {
			jobs++
		} else {
			web++
		}
	}
	if web < 8 || jobs < 5 {
		t.Errorf("%d web and %d job contexts, want at least 8 and 5", web, jobs)
	}
}

func TestPositionFor(t *testing.T) {
	for _, tc := range []struct {
		in      devtraffic.Position
		version int
		want    devtraffic.Position
	}{
		{devtraffic.Auto, 140000, devtraffic.Leading},
		{devtraffic.Auto, 170006, devtraffic.Leading},
		{devtraffic.Auto, 180000, devtraffic.Trailing},
		{devtraffic.Auto, 190000, devtraffic.Trailing},
		{devtraffic.Leading, 180000, devtraffic.Leading},
		{devtraffic.Trailing, 140000, devtraffic.Trailing},
	} {
		if got := devtraffic.PositionFor(tc.in, tc.version); got != tc.want {
			t.Errorf("PositionFor(%s, %d) = %s, want %s", tc.in, tc.version, got, tc.want)
		}
	}
}

// TestEveryStatementCommentMatchesDevWorkerRegexes renders every shape under
// every context that runs it, in both comment positions, many times with
// fresh random request metadata, and checks the comment's format and that
// dev/worker.json's regexes pull out exactly the intended controller, action,
// or job tag.
func TestEveryStatementCommentMatchesDevWorkerRegexes(t *testing.T) {
	reC, reA, reJ := devRegexes(t)
	sz := devtraffic.SizesFor(1)
	r := rand.New(rand.NewPCG(1, 2))
	pool := devtraffic.NewHostPool(r)
	for _, pos := range positions {
		for _, c := range devtraffic.Contexts() {
			for _, name := range c.Shapes {
				shape, _ := devtraffic.ShapeByName(name)
				for range 20 {
					meta := pool.NewRequest(r, c)
					stmt, _ := shape.Render(c, meta, devtraffic.ShardSchema(1+r.IntN(4)), pos, r, sz)

					keys, vals := commentFields(t, stmt, pos)
					if !slices.IsSorted(keys) {
						t.Fatalf("%s/%s: comment keys %v are not alphabetical", c.Label(), name, keys)
					}
					if !pidRE.MatchString(vals["pid"]) {
						t.Fatalf("%s/%s: pid %q", c.Label(), name, vals["pid"])
					}

					gotC, gotA, gotJ := lastGroup(reC, stmt), lastGroup(reA, stmt), lastGroup(reJ, stmt)
					if c.IsJob() {
						if want := []string{"context_id", "hostname", "job_tag", "pid"}; !slices.Equal(keys, want) {
							t.Fatalf("%s/%s: job comment keys %v, want %v", c.Label(), name, keys, want)
						}
						if !jobContextRE.MatchString(vals["context_id"]) {
							t.Fatalf("%s/%s: job context_id %q is not a 12-digit number", c.Label(), name, vals["context_id"])
						}
						if !jobHostRE.MatchString(vals["hostname"]) {
							t.Fatalf("%s/%s: job hostname %q", c.Label(), name, vals["hostname"])
						}
						if gotC != "" || gotA != "" || gotJ != c.JobTag {
							t.Fatalf("%s/%s: extracted (%q, %q, %q), want job tag %q only\n%s", c.Label(), name, gotC, gotA, gotJ, c.JobTag, stmt)
						}
					} else {
						if want := []string{"action", "context_id", "controller", "hostname", "pid"}; !slices.Equal(keys, want) {
							t.Fatalf("%s/%s: web comment keys %v, want %v", c.Label(), name, keys, want)
						}
						if !uuidRE.MatchString(vals["context_id"]) {
							t.Fatalf("%s/%s: web context_id %q is not a UUID", c.Label(), name, vals["context_id"])
						}
						if !webHostRE.MatchString(vals["hostname"]) {
							t.Fatalf("%s/%s: web hostname %q", c.Label(), name, vals["hostname"])
						}
						if gotC != c.Controller || gotA != c.Action || gotJ != "" {
							t.Fatalf("%s/%s: extracted (%q, %q, %q), want (%q, %q, \"\")\n%s", c.Label(), name, gotC, gotA, gotJ, c.Controller, c.Action, stmt)
						}
					}
				}
			}
		}
	}
}

func TestRequestMetadataVaries(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	pool := devtraffic.NewHostPool(r)
	var web, job devtraffic.Context
	for _, c := range devtraffic.Contexts() {
		if c.IsJob() {
			job = c
		} else {
			web = c
		}
	}
	for _, c := range []devtraffic.Context{web, job} {
		ids, hosts, pids := map[string]bool{}, map[string]bool{}, map[int]bool{}
		for range 200 {
			m := pool.NewRequest(r, c)
			ids[m.ContextID] = true
			hosts[m.Hostname] = true
			pids[m.PID] = true
		}
		if len(ids) != 200 {
			t.Errorf("%s: %d distinct context ids in 200 requests, want 200", c.Label(), len(ids))
		}
		if len(hosts) < 3 || len(hosts) > 10 {
			t.Errorf("%s: %d hostnames, want a small pool of 3 to 10", c.Label(), len(hosts))
		}
		if len(pids) <= len(hosts) || len(pids) > 40 {
			t.Errorf("%s: %d pids over %d hosts, want a small pool with more than one pid per host", c.Label(), len(pids), len(hosts))
		}
	}
}

// TestShapesAreDistinctFingerprintsAcrossShardsAndVariants checks the
// fingerprint spread: each shape is its own fingerprint, and a shape keeps
// one fingerprint across shard schemas and IN-list or VALUES lengths, which
// are what give it several pg_stat_statements entries.
func TestShapesAreDistinctFingerprintsAcrossShardsAndVariants(t *testing.T) {
	opts := devFingerprintOptions(t)
	sz := devtraffic.SizesFor(1)
	r := rand.New(rand.NewPCG(5, 6))
	pool := devtraffic.NewHostPool(r)
	owner := map[string]string{}
	for _, c := range devtraffic.Contexts() {
		for _, name := range c.Shapes {
			shape, _ := devtraffic.ShapeByName(name)
			var fp string
			for i := range 12 {
				stmt, _ := shape.Render(c, pool.NewRequest(r, c), devtraffic.ShardSchema(1+i%4), positions[i%2], r, sz)
				got, err := fingerprint.Normalized(stmt, opts)
				if err != nil {
					t.Fatalf("%s: fingerprint: %v\n%s", name, err, stmt)
				}
				if fp == "" {
					fp = got
				} else if got != fp {
					t.Fatalf("%s: fingerprint changed across renders:\n%s", name, stmt)
				}
			}
			if prev, ok := owner[fp]; ok && prev != name {
				t.Fatalf("shapes %s and %s share a fingerprint", prev, name)
			}
			owner[fp] = name
		}
	}
	if len(owner) != len(devtraffic.Shapes()) {
		t.Fatalf("%d fingerprints for %d shapes", len(owner), len(devtraffic.Shapes()))
	}
}
