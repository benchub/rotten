package main

import (
	"bytes"
	"io"
	"reflect"
	"regexp"
	"slices"
	"testing"

	"github.com/benchub/rotten/internal/docscheck"
)

const serverDoc = "docs/server.md"

// TestServerConfigDocumented checks that docs/server.md mentions every
// server config file key, flag of every subcommand, and environment
// variable rotten-server reads.
func TestServerConfigDocumented(t *testing.T) {
	var keys []string
	typ := reflect.TypeOf(ServeFileConfig{})
	for i := 0; i < typ.NumField(); i++ {
		keys = append(keys, typ.Field(i).Name)
	}
	docscheck.RequireDocumented(t, serverDoc, "server config keys (ServeFileConfig)", keys)

	var usage bytes.Buffer
	for _, args := range [][]string{
		{"migrate", "-h"},
		{"keys", "create", "-h"},
		{"keys", "list", "-h"},
		{"keys", "revoke", "-h"},
	} {
		run(args, io.Discard, &usage)
	}
	if _, err := loadServeConfig([]string{"-h"}, &usage); err == nil {
		t.Fatal("serve -h: want flag.ErrHelp")
	}
	flags := docscheck.FlagNames(usage.String())
	for _, want := range []string{"config", "dsn", "retention", "fqdn", "tls-cert"} {
		if !slices.Contains(flags, want) {
			t.Fatalf("flag extraction missed -%s; got %v", want, flags)
		}
	}
	docscheck.RequireFlagsDocumented(t, serverDoc, "rotten-server flags", flags)

	env := docscheck.GoStringLiterals(t, "cmd/rotten-server/*.go", regexp.MustCompile(`^ROTTEN_[A-Z0-9_]+$`))
	docscheck.RequireDocumented(t, serverDoc, "rotten-server environment variables", env)
}
