// Command rotten-server serves the HTTPS ingest API, applies schema migrations,
// and manages worker pass keys.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/benchub/rotten/internal/migrate"
)

const usage = `usage: rotten-server migrate [-dsn DSN] [-retention DAYS]
       rotten-server keys create|list|revoke ...
       rotten-server serve [-config FILE] [-listen ADDRESS] [-dsn DSN] -tls-cert FILE -tls-key FILE
       rotten-server --version

migrate applies every pending schema migration to the rotten database. Run it
as rotten_owner. The DSN comes from -dsn, or else ROTTEN_OWNER_DSN.

-retention (or ROTTEN_RETENTION) sets how long pg_partman keeps partitions of
events and event_context, as "21 days", "21d", or "504h". The default is 21
days. migrate reapplies it on every run, so a run that omits the setting
resets retention to 21 days. Pass the same value every time.
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 && (args[0] == "-version" || args[0] == "--version") {
		fmt.Fprintln(stdout, versionText("rotten-server"))
		return 0
	}
	if len(args) > 0 && (args[0] == "-help" || args[0] == "--help" || args[0] == "help") {
		fmt.Fprint(stdout, usage)
		return 0
	}
	if len(args) > 0 && args[0] == "serve" {
		return runServe(args[1:], stdout, stderr)
	}
	if len(args) > 0 && args[0] == "keys" {
		return runKeys(args[1:], stdout, stderr)
	}
	if len(args) == 0 || args[0] != "migrate" {
		fmt.Fprint(stderr, usage)
		return 2
	}
	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	// No env default: flag help prints defaults, and the DSN may hold a
	// password. The env var is read after Parse.
	dsn := fs.String("dsn", "", "rotten_owner DSN (default $ROTTEN_OWNER_DSN)")
	defRetention := os.Getenv("ROTTEN_RETENTION")
	if defRetention == "" {
		defRetention = migrate.DefaultRetention.String()
	}
	retention := fs.String("retention", defRetention, "partition retention, like \"21 days\" or \"21d\" (default $ROTTEN_RETENTION, else 21 days)")
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "rotten-server migrate: unexpected arguments %q\n", fs.Args())
		return 2
	}
	r, err := migrate.ParseRetention(*retention)
	if err != nil {
		fmt.Fprintf(stderr, "rotten-server migrate: %v\n", err)
		return 2
	}
	if *dsn == "" {
		*dsn = os.Getenv("ROTTEN_OWNER_DSN")
	}
	if *dsn == "" {
		fmt.Fprint(stderr, "rotten-server migrate: no DSN; pass -dsn or set ROTTEN_OWNER_DSN\n")
		return 2
	}
	applied, err := migrate.UpRetention(context.Background(), *dsn, r)
	for _, v := range applied {
		fmt.Fprintf(stdout, "applied migration %d\n", v)
	}
	if err != nil {
		fmt.Fprintf(stderr, "rotten-server migrate: %v\n", err)
		return 1
	}
	if len(applied) == 0 {
		fmt.Fprintln(stdout, "already up to date")
	}
	return 0
}
