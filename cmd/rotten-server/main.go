// Command rotten-server is the rotten server. For now it has one
// subcommand, migrate, which applies the embedded schema migrations.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/benchub/rotten/internal/migrate"
)

const usage = `usage: rotten-server migrate [-dsn DSN]

migrate applies every pending schema migration to the rotten database. Run it
as rotten_owner. The DSN comes from -dsn, or else ROTTEN_OWNER_DSN.
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "migrate" {
		fmt.Fprint(stderr, usage)
		return 2
	}
	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dsn := fs.String("dsn", os.Getenv("ROTTEN_OWNER_DSN"), "rotten_owner DSN (default $ROTTEN_OWNER_DSN)")
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	if *dsn == "" {
		fmt.Fprint(stderr, "rotten-server migrate: no DSN; pass -dsn or set ROTTEN_OWNER_DSN\n")
		return 2
	}
	applied, err := migrate.Up(context.Background(), *dsn)
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
