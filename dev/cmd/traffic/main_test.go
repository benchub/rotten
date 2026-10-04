package main

import (
	"flag"
	"io"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/devtraffic"
	"github.com/benchub/rotten/internal/docscheck"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestParseDefaults(t *testing.T) {
	cfg, err := parse(io.Discard, nil, env(nil))
	if err != nil {
		t.Fatal(err)
	}
	want := devtraffic.Config{
		AdminDSN: defaultAdminDSN,
		Shards:   4,
		Scale:    1,
		Rate:     1,
		Conns:    1,
		Comments: devtraffic.Auto,

		EpisodeEvery:  15 * time.Minute,
		EpisodeLength: 2 * time.Minute,
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("defaults %+v, want %+v", cfg, want)
	}
}

func TestParseEnvThenFlags(t *testing.T) {
	e := env(map[string]string{
		"TRAFFIC_ADMIN_DSN": "postgres://a@h/db",
		"TRAFFIC_RATE":      "5",
		"TRAFFIC_CONNS":     "3",
		"TRAFFIC_SHARDS":    "2",
		"TRAFFIC_SCALE":     "0.5",
		"TRAFFIC_COMMENTS":  "leading",
		"TRAFFIC_SEED":      "42",
		"TRAFFIC_DURATION":  "10s",

		"TRAFFIC_EPISODE_EVERY":  "5m",
		"TRAFFIC_EPISODE_LENGTH": "30s",
		"TRAFFIC_EPISODE":        "lock_wait",
	})
	cfg, err := parse(io.Discard, nil, e)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AdminDSN != "postgres://a@h/db" || cfg.Rate != 5 || cfg.Conns != 3 || cfg.Shards != 2 ||
		cfg.Scale != 0.5 || cfg.Comments != devtraffic.Leading || cfg.Seed != 42 || cfg.Duration != 10*time.Second ||
		cfg.EpisodeEvery != 5*time.Minute || cfg.EpisodeLength != 30*time.Second || cfg.Episode != devtraffic.LockWait {
		t.Fatalf("from env: %+v", cfg)
	}
	cfg, err = parse(io.Discard, []string{"-rate", "0.5", "-comments", "trailing", "-shards", "6", "-episode-every", "0", "-episode", "sleep"}, e)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Rate != 0.5 || cfg.Comments != devtraffic.Trailing || cfg.Shards != 6 || cfg.Conns != 3 ||
		cfg.EpisodeEvery != 0 || cfg.Episode != devtraffic.SlowSleep {
		t.Fatalf("flags over env: %+v", cfg)
	}
}

func TestParseRejectsBadValues(t *testing.T) {
	for _, tc := range []struct {
		args []string
		env  map[string]string
	}{
		{args: []string{"-comments", "middle"}},
		{env: map[string]string{"TRAFFIC_COMMENTS": "middle"}},
		{env: map[string]string{"TRAFFIC_RATE": "fast"}},
		{args: []string{"-rate", "0"}},
		{args: []string{"-conns", "-1"}},
		{args: []string{"-shards", "0"}},
		{args: []string{"stray"}},
		{args: []string{"-episode", "meltdown"}},
		{env: map[string]string{"TRAFFIC_EPISODE": "meltdown"}},
		{args: []string{"-episode-every", "-1m"}},
		{args: []string{"-episode-every", "2m", "-episode-length", "2m"}},
		{args: []string{"-episode-length", "0s"}},
	} {
		if _, err := parse(io.Discard, tc.args, env(tc.env)); err == nil {
			t.Errorf("parse(%q, %v) succeeded, want an error", tc.args, tc.env)
		}
	}
}

// TestTrafficDocumented checks that dev/README.md mentions every flag and
// environment variable the traffic command reads.
func TestTrafficDocumented(t *testing.T) {
	fs := flag.NewFlagSet("traffic", flag.ContinueOnError)
	register(fs, &devtraffic.Config{}, env(nil))
	var flags []string
	fs.VisitAll(func(f *flag.Flag) { flags = append(flags, f.Name) })
	docscheck.RequireFlagsDocumented(t, "dev/README.md", "traffic flags", flags)

	vars := docscheck.GoStringLiterals(t, "dev/cmd/traffic/*.go", regexp.MustCompile(`^TRAFFIC_[A-Z_]+$`))
	docscheck.RequireDocumented(t, "dev/README.md", "traffic environment variables", vars)
}
