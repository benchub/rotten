package auth

import (
	"errors"
	"fmt"
	"strings"
)

// FQDNMax is the longest pinned FQDN, after normalizing.
const FQDNMax = 253

// NormalizeFQDN checks a host name for pinning a key and returns it as
// stored: trimmed of ASCII whitespace and NUL (as Ruby's strip does),
// ASCII-lowercased, with one trailing dot dropped. Labels are letters,
// digits and inner hyphens, 63 characters at most. The UI applies the same
// rules (ui/app/models/api_key_issue.rb); ui/spec/fixtures/fqdn_vectors.json
// pins both.
func NormalizeFQDN(s string) (string, error) {
	in := s
	s = strings.Trim(s, " \t\n\v\f\r\x00")
	b := []byte(s)
	for i, c := range b {
		if 'A' <= c && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	s = strings.TrimSuffix(string(b), ".")
	if s == "" {
		return "", errors.New("fqdn is empty")
	}
	if len(s) > FQDNMax {
		return "", fmt.Errorf("fqdn %q is longer than %d characters", in, FQDNMax)
	}
	for _, label := range strings.Split(s, ".") {
		if !validLabel(label) {
			return "", fmt.Errorf("fqdn %q must be a host name such as db1.example.com", in)
		}
	}
	return s, nil
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
