package main

import (
	"encoding/json"
	"flag"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/benchub/rotten/internal/docscheck"
)

const workerDoc = "docs/worker.md"

// TestWorkerConfigDocumented checks that docs/worker.md mentions every
// worker config key (the Configuration struct and the sample conf), flag,
// and environment variable the worker reads.
func TestWorkerConfigDocumented(t *testing.T) {
	var keys []string
	typ := reflect.TypeOf(Configuration{})
	for i := 0; i < typ.NumField(); i++ {
		keys = append(keys, typ.Field(i).Name)
	}
	docscheck.RequireDocumented(t, workerDoc, "worker config keys (Configuration)", keys)

	b, err := os.ReadFile("../../conf")
	if err != nil {
		t.Fatal(err)
	}
	var sample map[string]json.RawMessage
	if err := json.Unmarshal(b, &sample); err != nil {
		t.Fatal(err)
	}
	var sampleKeys []string
	for k := range sample {
		sampleKeys = append(sampleKeys, k)
	}
	docscheck.RequireDocumented(t, workerDoc, "worker config keys (conf)", sampleKeys)

	var flags []string
	flag.CommandLine.VisitAll(func(f *flag.Flag) {
		if !strings.HasPrefix(f.Name, "test.") {
			flags = append(flags, f.Name)
		}
	})
	docscheck.RequireFlagsDocumented(t, workerDoc, "rotten-worker flags", flags)

	env := docscheck.GoStringLiterals(t, "cmd/rotten-worker/*.go", regexp.MustCompile(`^(ROTTEN|PG)[A-Z0-9_]+$`))
	docscheck.RequireDocumented(t, workerDoc, "rotten-worker environment variables", env)
}
