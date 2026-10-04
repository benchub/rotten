// Command traffic is the dev stack's traffic generator. It creates a small
// made-up LMS schema on the observed Postgres and runs a steady, varied load
// with production-style marginalia comments until stopped. See dev/README.md.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/benchub/rotten/internal/devtraffic"
)

const defaultAdminDSN = "postgres://postgres:postgres@localhost:5432/observed?sslmode=disable"

// envFor maps each flag to the environment variable that sets its default.
var envFor = map[string]string{
	"admin-dsn": "TRAFFIC_ADMIN_DSN",
	"rate":      "TRAFFIC_RATE",
	"conns":     "TRAFFIC_CONNS",
	"shards":    "TRAFFIC_SHARDS",
	"scale":     "TRAFFIC_SCALE",
	"comments":  "TRAFFIC_COMMENTS",
	"seed":      "TRAFFIC_SEED",
	"duration":  "TRAFFIC_DURATION",

	"episode-every":  "TRAFFIC_EPISODE_EVERY",
	"episode-length": "TRAFFIC_EPISODE_LENGTH",
	"episode":        "TRAFFIC_EPISODE",
}

type position struct{ p *devtraffic.Position }

func (v position) String() string {
	if v.p == nil {
		return ""
	}
	return string(*v.p)
}

func (v position) Set(s string) error {
	switch p := devtraffic.Position(s); p {
	case devtraffic.Auto, devtraffic.Leading, devtraffic.Trailing:
		*v.p = p
		return nil
	}
	return fmt.Errorf("want auto, leading or trailing")
}

type episode struct{ e *devtraffic.Episode }

func (v episode) String() string {
	if v.e == nil {
		return ""
	}
	return string(*v.e)
}

func (v episode) Set(s string) error {
	for _, e := range devtraffic.Episodes() {
		if devtraffic.Episode(s) == e {
			*v.e = e
			return nil
		}
	}
	if s == "" {
		*v.e = devtraffic.NoEpisode
		return nil
	}
	return fmt.Errorf("want one of %v, or empty for the schedule", devtraffic.Episodes())
}

// register defines the flags on fs, writing into cfg, and applies any
// environment variables in envFor, so explicit flags still win.
func register(fs *flag.FlagSet, cfg *devtraffic.Config, getenv func(string) string) error {
	cfg.Comments = devtraffic.Auto
	fs.StringVar(&cfg.AdminDSN, "admin-dsn", defaultAdminDSN, "DSN of a superuser on the observed Postgres, used to create the app roles, shard databases and tables")
	fs.Float64Var(&cfg.Rate, "rate", 1, "web requests and jobs started per second, on average; each runs 1 to 6 statements")
	fs.IntVar(&cfg.Conns, "conns", 1, "most connections per role per shard database")
	fs.IntVar(&cfg.Shards, "shards", 4, "shard databases (lms_shard_1 ...)")
	fs.Float64Var(&cfg.Scale, "scale", 1, "seed size multiplier for new shards (1 is 2000 users and 100 courses each)")
	fs.Var(position{&cfg.Comments}, "comments", "marginalia comment position: auto (leading before Postgres 18, trailing on 18+), leading or trailing")
	fs.Uint64Var(&cfg.Seed, "seed", 0, "random seed; 0 picks one from the clock")
	fs.DurationVar(&cfg.Duration, "duration", 0, "stop after this long; 0 runs until interrupted")
	fs.DurationVar(&cfg.EpisodeEvery, "episode-every", 15*time.Minute, "start a slow episode (slow_read, lock_wait and sleep in turn) at each multiple of this since the Unix epoch; 0 turns episodes off")
	fs.DurationVar(&cfg.EpisodeLength, "episode-length", 2*time.Minute, "how long each slow episode lasts; less than -episode-every")
	fs.Var(episode{&cfg.Episode}, "episode", "run this slow episode (slow_read, lock_wait or sleep) for the whole run instead of the schedule")
	for name, key := range envFor {
		if v := getenv(key); v != "" {
			if err := fs.Set(name, v); err != nil {
				return fmt.Errorf("%s=%q: %w", key, v, err)
			}
		}
	}
	return nil
}

// parse builds the generator config from args and the environment.
func parse(stderr io.Writer, args []string, getenv func(string) string) (devtraffic.Config, error) {
	var cfg devtraffic.Config
	fs := flag.NewFlagSet("traffic", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if err := register(fs, &cfg, getenv); err != nil {
		return cfg, err
	}
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	switch {
	case fs.NArg() > 0:
		return cfg, fmt.Errorf("unexpected arguments %q", fs.Args())
	case cfg.Rate <= 0:
		return cfg, errors.New("-rate must be positive")
	case cfg.Conns < 1:
		return cfg, errors.New("-conns must be at least 1")
	case cfg.Shards < 1:
		return cfg, errors.New("-shards must be at least 1")
	case cfg.Scale <= 0:
		return cfg, errors.New("-scale must be positive")
	case cfg.EpisodeEvery < 0:
		return cfg, errors.New("-episode-every must not be negative")
	case cfg.EpisodeEvery > 0 && (cfg.EpisodeLength <= 0 || cfg.EpisodeLength >= cfg.EpisodeEvery):
		return cfg, errors.New("-episode-length must be positive and less than -episode-every")
	}
	return cfg, nil
}

func main() {
	cfg, err := parse(os.Stderr, os.Args[1:], os.Getenv)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		log.Fatalf("traffic: %v", err)
	}
	cfg.Logf = log.Printf
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	for {
		_, err := devtraffic.Run(ctx, cfg)
		if ctx.Err() != nil || (err == nil && cfg.Duration > 0) {
			return
		}
		// Setup or connecting failed, e.g. Postgres restarting: try again.
		log.Printf("traffic: %v; retrying in 5s", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}
