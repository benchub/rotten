package auth

import (
	"errors"
	"fmt"
	"strings"
)

// FQDNMax is the longest pinned FQDN, after normalizing.
const FQDNMax = 253

// SuffixPrefix starts a suffix pin: "*.db.example.com" allows any host with
// one or more labels before "db.example.com", but not that name itself.
const SuffixPrefix = "*."

// NormalizeFQDN checks a pin for a key and returns it as stored: trimmed of
// ASCII whitespace and NUL (as Ruby's strip does), ASCII-lowercased, with one
// trailing dot dropped. A pin is a host name, or SuffixPrefix and a domain of
// at least two labels whose last label isn't all digits, so neither "*.com"
// nor part of an IPv4 address can be pinned. Labels are letters, digits and
// inner hyphens, 63 characters at most. The UI applies the same rules
// (ui/app/models/api_key_issue.rb); ui/spec/fixtures/fqdn_vectors.json pins
// both.
func NormalizeFQDN(s string) (string, error) {
	in := s
	s = strings.Trim(s, " \t\n\v\f\r\x00")
	s = strings.TrimSuffix(asciiLower(s), ".")
	if s == "" {
		return "", errors.New("fqdn is empty")
	}
	if len(s) > FQDNMax {
		return "", fmt.Errorf("fqdn %q is longer than %d characters", in, FQDNMax)
	}
	host, suffix := strings.CutPrefix(s, SuffixPrefix)
	labels := strings.Split(host, ".")
	if suffix && (len(labels) < 2 || allDigits(labels[len(labels)-1])) {
		return "", fmt.Errorf("fqdn %q must be *. and a domain of two or more labels, such as *.db.example.com", in)
	}
	for _, label := range labels {
		if !validLabel(label) {
			return "", fmt.Errorf("fqdn %q must be a host name such as db1.example.com, or a suffix pin such as *.db.example.com", in)
		}
	}
	return s, nil
}

// asciiLower lowercases ASCII letters only, leaving other bytes alone.
func asciiLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if 'A' <= c && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}

func allDigits(l string) bool {
	return strings.Trim(l, "0123456789") == ""
}

func validLabel(l string) bool {
	if l == "" || len(l) > 63 || l[0] == '-' || l[len(l)-1] == '-' {
		return false
	}
	for i := 0; i < len(l); i++ {
		c := l[i]
		if !('a' <= c && c <= 'z' || '0' <= c && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}
