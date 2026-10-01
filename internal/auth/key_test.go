package auth

import (
	"strings"
	"testing"
)

func TestNewSecretFormat(t *testing.T) {
	s1, h1, err := NewSecret()
	if err != nil {
		t.Fatal(err)
	}
	s2, _, _ := NewSecret()
	if s1 == s2 {
		t.Fatal("two secrets are equal")
	}
	// 32 bytes, unpadded URL-safe base64, is 43 characters.
	if len(s1) < 43 || strings.ContainsAny(s1, "+/=") {
		t.Errorf("secret %q isn't 32+ bytes of URL-safe base64", s1)
	}
	if h1 != HashSecret(s1) || len(h1) != 64 {
		t.Errorf("hash %q doesn't match HashSecret", h1)
	}
	tok := FormatKey(12, s1)
	id, sec, err := ParseKey(tok)
	if err != nil || id != 12 || sec != s1 {
		t.Errorf("ParseKey(%q) = %d, %q, %v", tok, id, sec, err)
	}
}

func TestParseKeyRejectsMalformed(t *testing.T) {
	good := strings.Repeat("A", 43)
	for _, tok := range []string{
		"", "rotten_", "rotten__" + good, "rotten_x_" + good, "rotten_0_" + good,
		"rotten_-1_" + good, "rotten_12", "rotten_12_", "rotten_12_short",
		"other_12_" + good, "rotten_12_" + good + "!", "rotten_+12_" + good,
		"rotten_99999999999999999999_" + good,
	} {
		if _, _, err := ParseKey(tok); err == nil {
			t.Errorf("ParseKey(%q) succeeded, want an error", tok)
		}
	}
}

func TestKeyAllowsFQDN(t *testing.T) {
	pinned := Key{ID: 1, FQDN: "db1.example.com"}
	if !pinned.AllowsFQDN("db1.example.com") || !pinned.AllowsFQDN("DB1.example.com.") {
		t.Error("pinned key rejects its own fqdn")
	}
	if pinned.AllowsFQDN("db2.example.com") || pinned.AllowsFQDN("") {
		t.Error("pinned key allows another fqdn")
	}
	if (Key{ID: 2}).AllowsFQDN("anything.example.com") {
		t.Error("unpinned key allows an fqdn; unpinned keys may not register sources")
	}
}
