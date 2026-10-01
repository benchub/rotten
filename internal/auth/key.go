// Package auth checks worker pass keys and manages them.
//
// A pass key looks like rotten_<key id>_<secret>. The key id is the
// api_keys.id in decimal. The secret is 32 bytes from crypto/rand, encoded
// as unpadded URL-safe base64, so it may itself contain "_". The database
// stores only hex(sha256(secret)). A plain hash is enough for a
// high-entropy random secret; a slow password hash would buy nothing.
//
// Source binding. A key pinned to an fqdn may act only for that host.
// A key with no pinned fqdn authenticates, but AllowsFQDN reports false
// for every host, so Register and SubmitHarvest refuse it with
// PermissionDenied. That fails closed: api_keys has no other link to a
// source, so an unpinned key could otherwise write as any host. Workers
// need keys created with --fqdn.
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
)

const (
	prefix      = "rotten_"
	secretBytes = 32
)

var errMalformed = errors.New("malformed pass key")

// Key is an authenticated pass key. It never holds the secret.
type Key struct {
	ID   int64
	Name string
	// FQDN is the host the key is pinned to, or "" if it isn't pinned.
	FQDN string
}

// AllowsFQDN reports whether the key may act for host. An unpinned key
// allows no host (see the package comment).
func (k Key) AllowsFQDN(host string) bool {
	if k.FQDN == "" {
		return false
	}
	return normFQDN(k.FQDN) == normFQDN(host)
}

func normFQDN(s string) string { return strings.TrimSuffix(strings.ToLower(s), ".") }

// NewSecret returns a fresh random secret and its stored hash.
func NewSecret() (secret, hash string, err error) {
	b := make([]byte, secretBytes)
	if _, err := rand.Read(b); err != nil {
		return "", "", err
	}
	secret = base64.RawURLEncoding.EncodeToString(b)
	return secret, HashSecret(secret), nil
}

// HashSecret returns hex(sha256(secret)), the form api_keys.secret_hash holds.
func HashSecret(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// FormatKey builds the token a worker sends.
func FormatKey(id int64, secret string) string {
	return prefix + strconv.FormatInt(id, 10) + "_" + secret
}

// ParseKey splits a token into its key id and secret. Errors never include
// the token.
func ParseKey(tok string) (id int64, secret string, err error) {
	rest, ok := strings.CutPrefix(tok, prefix)
	if !ok {
		return 0, "", errMalformed
	}
	idStr, secret, ok := strings.Cut(rest, "_")
	if !ok || idStr == "" || idStr[0] < '1' || idStr[0] > '9' {
		return 0, "", errMalformed
	}
	id, err = strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		return 0, "", errMalformed
	}
	b, err := base64.RawURLEncoding.DecodeString(secret)
	if err != nil || len(b) < secretBytes {
		return 0, "", errMalformed
	}
	return id, secret, nil
}
