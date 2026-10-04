package auth_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/benchub/rotten/internal/auth"
)

// ui/spec/models/api_key_issue_fqdn_spec.rb checks the UI against the same
// vectors, so CLI and UI keys are pinned the same way.
type fqdnVectors struct {
	Valid []struct {
		Input string `json:"input"`
		FQDN  string `json:"fqdn"`
	} `json:"valid"`
	Invalid []string `json:"invalid"`
}

func loadFQDNVectors(t *testing.T) fqdnVectors {
	t.Helper()
	var v fqdnVectors
	if err := json.Unmarshal(uiFile(t, "spec", "fixtures", "fqdn_vectors.json"), &v); err != nil {
		t.Fatal(err)
	}
	if len(v.Valid) < 5 || len(v.Invalid) < 5 {
		t.Fatalf("got %d valid and %d invalid fqdn vectors, want at least 5 of each", len(v.Valid), len(v.Invalid))
	}
	return v
}

func TestNormalizeFQDNVectorsSharedWithUI(t *testing.T) {
	v := loadFQDNVectors(t)
	for _, c := range v.Valid {
		got, err := auth.NormalizeFQDN(c.Input)
		if err != nil || got != c.FQDN {
			t.Errorf("NormalizeFQDN(%q) = %q, %v; want %q", c.Input, got, err, c.FQDN)
		}
	}
	for _, in := range v.Invalid {
		if got, err := auth.NormalizeFQDN(in); err == nil {
			t.Errorf("NormalizeFQDN(%q) = %q, want an error", in, got)
		}
	}
}

func TestCreateKeyNormalizesFQDN(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	k, err := auth.CreateKey(ctx, f.owner, "mixed", " DB1.Example.COM. ", "test")
	if err != nil {
		t.Fatal(err)
	}
	var stored string
	if err := f.owner.QueryRow(ctx, "select fqdn from rotten.api_keys where id = $1", k.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != "db1.example.com" {
		t.Errorf("stored fqdn %q, want db1.example.com", stored)
	}

	u, err := auth.CreateKey(ctx, f.owner, "unpinned", "", "test")
	if err != nil {
		t.Fatal(err)
	}
	var null bool
	if err := f.owner.QueryRow(ctx, "select fqdn is null from rotten.api_keys where id = $1", u.ID).Scan(&null); err != nil || !null {
		t.Errorf("empty fqdn: null %v, %v; want an unpinned key", null, err)
	}
}

func TestCreateKeyRejectsInvalidFQDN(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	for _, in := range []string{"db_1.example.com", ".", " ", "*.example.com", "db1.example.com:5432"} {
		if _, err := auth.CreateKey(ctx, f.owner, "bad", in, "test"); err == nil || !strings.Contains(err.Error(), "fqdn") {
			t.Errorf("CreateKey(fqdn %q) = %v, want an fqdn error", in, err)
		}
	}
	var n int
	if err := f.owner.QueryRow(ctx, "select count(*) from rotten.api_keys").Scan(&n); err != nil || n != 0 {
		t.Errorf("%d keys stored, %v; want none", n, err)
	}
}
