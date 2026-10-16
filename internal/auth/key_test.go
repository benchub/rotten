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

func TestKeySuffixPinAllowsFQDN(t *testing.T) {
	k := Key{ID: 3, FQDN: "*.example.com"}
	for _, h := range []string{"db1.example.com", "a.b.example.com", "DB1.Example.COM", "db1.example.com."} {
		if !k.AllowsFQDN(h) {
			t.Errorf("*.example.com refuses %q", h)
		}
	}
	for _, h := range []string{
		"example.com", "example.com.", ".example.com", "..example.com", "badexample.com", "db1.badexample.com",
		"db1.example.com.evil", "db1.example.co", "*.example.com", "x.*.example.com", "db_1.example.com",
		"db1..example.com", "db1.example.com..", " db1.example.com", "", "com",
		"\u212a.example.com", "\u0130.example.com", "db1.ex\u00e4mple.example.com", "d\u00e91.example.com",
	} {
		if k.AllowsFQDN(h) {
			t.Errorf("*.example.com allows %q", h)
		}
	}
	if !(Key{ID: 4, FQDN: "*.Example.COM."}).AllowsFQDN("db1.example.com") {
		t.Error("suffix pin match depends on the pin's case or trailing dot")
	}
	// An exact pin is compared as a string: "*" in a host is never a wildcard.
	if (Key{ID: 5, FQDN: "db1.example.com"}).AllowsFQDN("*.example.com") {
		t.Error("exact pin allows a wildcard host")
	}
}
