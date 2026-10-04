package main

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/benchub/rotten/internal/auth"
	"github.com/benchub/rotten/internal/testdb"
)

func TestKeysRoundTrip(t *testing.T) {
	db := testdb.StartRotten(t)
	t.Setenv("ROTTEN_ADMIN_DSN", db.DSNAs(t, testdb.OwnerRole))

	keys := func(args ...string) (string, string, int) {
		var out, errb bytes.Buffer
		code := run(append([]string{"keys"}, args...), &out, &errb)
		return out.String(), errb.String(), code
	}

	out, errs, code := keys("create", "--fqdn", "db1.example.com", "w1")
	if code != 0 {
		t.Fatalf("create: %d %s", code, errs)
	}
	tok := regexp.MustCompile(`rotten_\d+_[A-Za-z0-9_-]{43,}`).FindString(out)
	if tok == "" {
		t.Fatalf("create printed no key: %q", out)
	}
	_, secret, err := auth.ParseKey(tok)
	if err != nil {
		t.Fatal(err)
	}
	if _, errs, code := keys("create", "w2"); code != 0 {
		t.Fatalf("create w2: %d %s", code, errs)
	}
	if _, _, code := keys("create", "w1"); code == 0 {
		t.Error("duplicate name succeeded")
	}

	out, errs, code = keys("list")
	if code != 0 {
		t.Fatalf("list: %d %s", code, errs)
	}
	if !strings.Contains(out, "w1") || !strings.Contains(out, "db1.example.com") || !strings.Contains(out, "w2") {
		t.Errorf("list missing keys:\n%s", out)
	}
	if strings.Contains(out, secret) || strings.Contains(out, auth.HashSecret(secret)) {
		t.Errorf("list shows a secret or hash:\n%s", out)
	}

	if _, errs, code := keys("revoke", "w1"); code != 0 {
		t.Fatalf("revoke: %d %s", code, errs)
	}
	if _, _, code := keys("revoke", "w1"); code == 0 {
		t.Error("revoking twice succeeded")
	}
	if _, _, code := keys("revoke", "nope"); code == 0 {
		t.Error("revoking an unknown key succeeded")
	}
	out, _, _ = keys("list")
	var w1, w2 string
	for _, l := range strings.Split(out, "\n") {
		f := strings.Fields(l)
		if len(f) > 1 && f[1] == "w1" {
			w1 = l
		}
		if len(f) > 1 && f[1] == "w2" {
			w2 = l
		}
	}
	if !strings.Contains(w1, "revoked") || strings.Contains(w2, "revoked") {
		t.Errorf("list after revoke:\n%s", out)
	}
}

// --fqdn follows the UI's rules: bad names are refused before anything is
// stored, and case and a trailing dot are normalized away.
func TestKeysCreateFQDN(t *testing.T) {
	db := testdb.StartRotten(t)
	t.Setenv("ROTTEN_ADMIN_DSN", db.DSNAs(t, testdb.OwnerRole))
	keys := func(args ...string) (string, string, int) {
		var out, errb bytes.Buffer
		code := run(append([]string{"keys"}, args...), &out, &errb)
		return out.String(), errb.String(), code
	}

	for i, bad := range []string{"db_1.example.com", "*.example.com", "db1..example.com", "."} {
		out, errs, code := keys("create", "--fqdn", bad, fmt.Sprintf("bad%d", i))
		if code == 0 || !strings.Contains(errs, "fqdn") || strings.Contains(out, "rotten_") {
			t.Errorf("create --fqdn %q: code %d, stdout %q, stderr %q; want an fqdn error and no key", bad, code, out, errs)
		}
	}
	if _, errs, code := keys("create", "--fqdn", "DB1.Example.COM.", "mixed"); code != 0 {
		t.Fatalf("create mixed: %d %s", code, errs)
	}
	out, _, _ := keys("list")
	if strings.Contains(out, "bad") {
		t.Errorf("a key with a bad fqdn was stored:\n%s", out)
	}
	var row string
	for _, l := range strings.Split(out, "\n") {
		if f := strings.Fields(l); len(f) > 2 && f[1] == "mixed" {
			row = l
			if f[2] != "db1.example.com" {
				t.Errorf("mixed stored fqdn %q, want db1.example.com", f[2])
			}
		}
	}
	if row == "" {
		t.Errorf("list has no mixed key:\n%s", out)
	}
}

// Help and flag errors print flag defaults, so a DSN default would leak its
// password.
func TestUsageDoesNotLeakDSN(t *testing.T) {
	const pw = "s3cretpassw0rd"
	dsn := "postgres://rotten_owner:" + pw + "@nowhere.invalid/rotten"
	t.Setenv("ROTTEN_ADMIN_DSN", dsn)
	t.Setenv("ROTTEN_OWNER_DSN", dsn)
	for _, args := range [][]string{
		{"keys", "list", "-h"}, {"keys", "create", "--bogus"}, {"keys", "revoke", "-h"},
		{"migrate", "-h"}, {"migrate", "--bogus"},
	} {
		var out, errb bytes.Buffer
		run(args, &out, &errb)
		if strings.Contains(out.String()+errb.String(), pw) {
			t.Errorf("run(%q) printed the DSN password", args)
		}
	}
}

func TestKeysUsage(t *testing.T) {
	t.Setenv("ROTTEN_ADMIN_DSN", "")
	for _, args := range [][]string{{"keys"}, {"keys", "bogus"}, {"keys", "create"}, {"keys", "list"}, {"keys", "revoke"}} {
		var out, errb bytes.Buffer
		if code := run(args, &out, &errb); code != 2 {
			t.Errorf("run(%q) = %d, want 2; stderr %q", args, code, errb.String())
		}
	}
}
