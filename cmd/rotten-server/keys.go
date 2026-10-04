package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/user"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/benchub/rotten/internal/auth"
)

const keysUsage = `usage: rotten-server keys create [--fqdn HOST] NAME
       rotten-server keys list
       rotten-server keys revoke NAME

keys manages worker pass keys. It connects as rotten_owner using
ROTTEN_ADMIN_DSN (or -dsn).

create prints the new key once. It isn't stored and can't be shown again.
--fqdn pins the key to one worker host. Register and SubmitHarvest refuse a
key without a pinned fqdn, so every worker key needs --fqdn. HOST must be a
host name such as db1.example.com, checked as the UI checks it; it's stored
lowercased, without a trailing dot.
list never shows secrets. revoke takes effect within the server's key cache
TTL (30 seconds by default).
`

func runKeys(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, keysUsage)
		return 2
	}
	cmd := args[0]
	fs := flag.NewFlagSet("keys "+cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	// No env default here: flag help prints defaults, and the DSN may hold
	// a password. The env var is read after Parse.
	dsn := fs.String("dsn", "", "rotten_owner DSN (default $ROTTEN_ADMIN_DSN)")
	var fqdn *string
	nargs := 1
	switch cmd {
	case "create":
		fqdn = fs.String("fqdn", "", "pin the key to this worker host")
	case "revoke":
	case "list":
		nargs = 0
	default:
		fmt.Fprint(stderr, keysUsage)
		return 2
	}
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	if fs.NArg() != nargs {
		fmt.Fprint(stderr, keysUsage)
		return 2
	}
	if fqdn != nil && *fqdn != "" {
		if _, err := auth.NormalizeFQDN(*fqdn); err != nil {
			fmt.Fprintf(stderr, "rotten-server keys: %v\n", err)
			return 2
		}
	}
	if *dsn == "" {
		*dsn = os.Getenv("ROTTEN_ADMIN_DSN")
	}
	if *dsn == "" {
		fmt.Fprint(stderr, "rotten-server keys: no DSN; pass -dsn or set ROTTEN_ADMIN_DSN\n")
		return 2
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		fmt.Fprintf(stderr, "rotten-server keys: connect: %v\n", err)
		return 1
	}
	defer conn.Close(ctx)

	switch cmd {
	case "create":
		c, err := auth.CreateKey(ctx, conn, fs.Arg(0), *fqdn, operator())
		if err != nil {
			fmt.Fprintf(stderr, "rotten-server keys: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "created key %d (%s). This is the only time the key is shown:\n%s\n", c.ID, fs.Arg(0), c.Token)
	case "revoke":
		if err := auth.RevokeKey(ctx, conn, fs.Arg(0), operator()); err != nil {
			fmt.Fprintf(stderr, "rotten-server keys: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "revoked key %s\n", fs.Arg(0))
	case "list":
		keys, err := auth.ListKeys(ctx, conn)
		if err != nil {
			fmt.Fprintf(stderr, "rotten-server keys: %v\n", err)
			return 1
		}
		w := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tFQDN\tCREATED\tCREATED BY\tLAST USED\tSTATUS")
		for _, k := range keys {
			status := "active"
			if k.RevokedAt != nil {
				status = "revoked " + k.RevokedAt.UTC().Format(time.RFC3339) + " by " + deref(k.RevokedBy)
			}
			last := "never"
			if k.LastUsedAt != nil {
				last = k.LastUsedAt.UTC().Format(time.RFC3339)
			}
			f := deref(k.FQDN)
			if f == "" {
				f = "(unpinned)"
			}
			fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t%s\t%s\n", k.ID, k.Name, f, k.CreatedAt.UTC().Format(time.RFC3339), k.CreatedBy, last, status)
		}
		w.Flush()
	}
	return 0
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// operator names who ran the command, for created_by and revoked_by.
func operator() string {
	if u, err := user.Current(); err == nil && u.Username != "" {
		return u.Username
	}
	if u := os.Getenv("USER"); u != "" {
		return u
	}
	return "unknown"
}
