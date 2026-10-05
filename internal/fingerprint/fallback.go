package fingerprint

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/benchub/rotten/internal/harvestlimits"
)

// ErrParse is Normalized's error when the parser rejects the statement, as
// the pinned Postgres 17 parser does for some Postgres 18 syntax. Callers use
// Fallback for those statements.
var ErrParse = errors.New("failed to parse")

// FallbackPrefix starts every fallback fingerprint. pg_query fingerprints are
// 16 lowercase hex digits, so no fallback can equal one.
const FallbackPrefix = harvestlimits.FallbackFingerprintPrefix

// MaxFallbackTextBytes caps FallbackResult.Text so it fits the server's
// limit on FingerprintAggregate.normalized.
const MaxFallbackTextBytes = harvestlimits.MaxNormalizedBytes

// FallbackResult is a text-derived fingerprint for a statement the parser
// rejects, and the text to store with it.
type FallbackResult struct {
	// Fingerprint is FallbackPrefix plus the first 16 hex digits of the
	// SHA-256 of Text before it's cut to MaxFallbackTextBytes.
	Fingerprint string
	// Text is the statement with its leading and trailing comments,
	// whitespace and semicolons stripped, cut to MaxFallbackTextBytes.
	Text string
}

// Fallback fingerprints a statement the parser rejects by its text.
//
// It hashes the pg_stat_statements text, which already has constants
// replaced by $n, so it doesn't depend on the observed server's OIDs or on
// pg_stat_statements' queryid, which is OID-based on Postgres 14 through 17.
// The same text gets the same fingerprint on every worker, server and
// restart. Leading and trailing comments are stripped, so marginalia doesn't
// split a statement; anything else that changes the text does, including
// whitespace, schema qualification, a comment in the middle, and a
// different number of $n placeholders. Generated cursor and temp-table names
// aren't collapsed: a pattern can't find each name in arbitrary SQL without
// lexing it, so each name gets its own fallback.
//
// A fallback never equals a parsed fingerprint, so when a later parser
// accepts the statement its history starts again under a new fingerprint.
func Fallback(query string) FallbackResult {
	text := stripOuterComments(query)
	sum := sha256.Sum256([]byte(text))
	return FallbackResult{
		Fingerprint: FallbackPrefix + hex.EncodeToString(sum[:8]),
		Text:        truncateUTF8(text, MaxFallbackTextBytes),
	}
}

// IsFallback reports whether fingerprint came from Fallback.
func IsFallback(fingerprint string) bool {
	return strings.HasPrefix(fingerprint, FallbackPrefix)
}

func truncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// stripOuterComments drops the comments, whitespace and semicolons before
// the statement's first token and after its last. It lexes just enough SQL
// (quoted strings and identifiers, dollar quotes, nested block comments) not
// to mistake a comment marker inside a literal for a comment. If nothing but
// comments is left, it returns the trimmed query unchanged.
func stripOuterComments(q string) string {
	for {
		first, last := contentSpan(q)
		if first < 0 {
			return strings.TrimSpace(q)
		}
		trimmed := q[first:last]
		next := strings.TrimRight(trimmed, "; \t\r\n\f\v")
		if next == trimmed {
			return trimmed
		}
		if next == "" {
			return strings.TrimSpace(q)
		}
		q = next
	}
}

// contentSpan returns the byte range from the first to the last character
// that isn't whitespace or in a comment, or -1, 0 if there is none.
func contentSpan(q string) (int, int) {
	first, last := -1, 0
	mark := func(start, end int) {
		if first < 0 {
			first = start
		}
		last = end
	}
	n := len(q)
	for i := 0; i < n; {
		c := q[i]
		switch {
		case isSQLSpace(c):
			i++
		case c == '-' && i+1 < n && q[i+1] == '-':
			// Postgres ends a line comment at either.
			end := strings.IndexAny(q[i:], "\r\n")
			if end < 0 {
				i = n
			} else {
				i += end + 1
			}
		case c == '/' && i+1 < n && q[i+1] == '*':
			i = blockCommentEnd(q, i)
		case c == '\'':
			escapes := i > 0 && (q[i-1] == 'E' || q[i-1] == 'e') && (i < 2 || !isIdentByte(q[i-2]))
			end := quotedEnd(q, i, '\'', escapes)
			mark(i, end)
			i = end
		case c == '"':
			end := quotedEnd(q, i, '"', false)
			mark(i, end)
			i = end
		case c == '$' && (i == 0 || !isIdentByte(q[i-1])):
			end := dollarQuoteEnd(q, i)
			mark(i, end)
			i = end
		default:
			mark(i, i+1)
			i++
		}
	}
	return first, last
}

func isSQLSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'
}

func isIdentByte(c byte) bool {
	return c == '_' || c == '$' || c >= 0x80 ||
		(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// blockCommentEnd returns the index just past the block comment starting at
// i, which may nest. An unterminated comment runs to the end.
func blockCommentEnd(q string, i int) int {
	depth := 0
	for i < len(q) {
		switch {
		case strings.HasPrefix(q[i:], "/*"):
			depth++
			i += 2
		case strings.HasPrefix(q[i:], "*/"):
			depth--
			i += 2
			if depth == 0 {
				return i
			}
		default:
			i++
		}
	}
	return len(q)
}

// quotedEnd returns the index just past the quoted string or identifier
// starting at i. A doubled quote is part of it, and so is a backslash-escaped
// one in an E” string.
func quotedEnd(q string, i int, quote byte, escapes bool) int {
	for j := i + 1; j < len(q); j++ {
		switch {
		case escapes && q[j] == '\\':
			j++
		case q[j] == quote:
			if j+1 < len(q) && q[j+1] == quote {
				j++
				continue
			}
			return j + 1
		}
	}
	return len(q)
}

// dollarQuoteEnd returns the index just past the dollar-quoted string at i,
// or i+1 if the $ doesn't open one (as in a $1 parameter).
func dollarQuoteEnd(q string, i int) int {
	j := i + 1
	if j < len(q) && q[j] >= '0' && q[j] <= '9' {
		return i + 1
	}
	for j < len(q) && q[j] != '$' && isIdentByte(q[j]) {
		j++
	}
	if j >= len(q) || q[j] != '$' {
		return i + 1
	}
	tag := q[i : j+1]
	end := strings.Index(q[j+1:], tag)
	if end < 0 {
		return len(q)
	}
	return j + 1 + end + len(tag)
}
