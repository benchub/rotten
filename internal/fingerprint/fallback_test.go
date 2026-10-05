package fingerprint

import (
	"errors"
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"
)

const pg18Update = "update widgets set name = $1 where id = $2 returning with (old as o, new as n) o.name, n.name"

// The fallback is a pure function of the text, so it's the same on every
// worker and across restarts. Pin one value so a change to it is a decision,
// not an accident: it would split every unparsed statement's history.
func TestFallbackIsPinned(t *testing.T) {
	got := Fallback(pg18Update)
	// printf '%s' "$pg18Update" | shasum -a 256 | cut -c1-16
	if want := "unparsed-a4b6e7cfa5bf9da0"; got.Fingerprint != want {
		t.Fatalf("Fallback(%q) = %q, want %q", pg18Update, got.Fingerprint, want)
	}
	if got.Text != pg18Update {
		t.Fatalf("Text = %q, want %q", got.Text, pg18Update)
	}
}

func TestFallbackIgnoresLeadingAndTrailingCommentsAndWhitespace(t *testing.T) {
	want := Fallback(pg18Update).Fingerprint
	for _, q := range []string{
		"  " + pg18Update + "\n",
		pg18Update + ";",
		pg18Update + " ; ",
		"/*controller:users,action:update*/ " + pg18Update,
		pg18Update + " /*controller:users,action:update*/",
		pg18Update + " /*controller:a*/ /* job:b */",
		"-- leading line comment\n" + pg18Update,
		pg18Update + " -- trailing line comment",
		"/* outer /* nested */ still comment */" + pg18Update,
		"/*a*/\n\t/*b*/" + pg18Update + "/*c*/\n--d\n",
	} {
		got := Fallback(q)
		if got.Fingerprint != want || got.Text != pg18Update {
			t.Errorf("Fallback(%q) = %+v, want %s with text %q", q, got, want, pg18Update)
		}
	}
}

func TestFallbackKeepsWhatIsntALeadingOrTrailingComment(t *testing.T) {
	base := Fallback(pg18Update).Fingerprint
	for _, q := range []string{
		// Different statements.
		strings.Replace(pg18Update, "widgets", "gadgets", 1),
		strings.Replace(pg18Update, "o.name", "o.id", 1),
		// Whitespace inside the statement is part of its text.
		strings.Replace(pg18Update, "set name", "set  name", 1),
		// A comment in the middle isn't marginalia at either end.
		strings.Replace(pg18Update, "where", "/* mid */ where", 1),
	} {
		if got := Fallback(q).Fingerprint; got == base {
			t.Errorf("Fallback(%q) = %s, the same as the base statement's", q, got)
		}
	}
	for q, text := range map[string]string{
		"select '/* not a comment */'":          "select '/* not a comment */'",
		"select 'it''s -- not a comment'":       "select 'it''s -- not a comment'",
		`select E'\' -- still a string'`:        `select E'\' -- still a string'`,
		`select "a -- b" from t`:                `select "a -- b" from t`,
		"select $tag$ -- body */ $tag$":         "select $tag$ -- body */ $tag$",
		"select $$ /* body $$ /* tail */":       "select $$ /* body $$",
		"select $1 /*, ... */ from t /*ctx:x*/": "select $1 /*, ... */ from t",
		"/*only a comment*/":                    "/*only a comment*/",
		"select 1 /* unterminated":              "select 1",
		"select a-1 from t":                     "select a-1 from t",
		"select a/2 from t":                     "select a/2 from t",
		"select x from t where y = $1--comment": "select x from t where y = $1",
	} {
		if got := Fallback(q).Text; got != text {
			t.Errorf("Fallback(%q).Text = %q, want %q", q, got, text)
		}
	}
}

// Postgres ends a -- comment at a carriage return as well as a newline, so
// what follows a CR-ended comment is part of the statement.
func TestFallbackEndsLineCommentsAtCarriageReturn(t *testing.T) {
	a := Fallback("UPDATE t SET x=$1 -- context:a\r RETURNING WITH (OLD AS o) o.x")
	b := Fallback("UPDATE t SET x=$1 -- context:b\r RETURNING WITH (NEW AS n) n.x")
	if a.Fingerprint == b.Fingerprint {
		t.Errorf("distinct statements after CR-ended comments share %s: %q, %q", a.Fingerprint, a.Text, b.Text)
	}
	for q, text := range map[string]string{
		"UPDATE t SET x=$1 -- context:a\r RETURNING WITH (OLD AS o) o.x": "UPDATE t SET x=$1 -- context:a\r RETURNING WITH (OLD AS o) o.x",
		"-- lead\rselect 1":             "select 1",
		"-- lead\r\nselect 1 -- tail\r": "select 1",
		"select 1 -- tail\r\n":          "select 1",
	} {
		if got := Fallback(q).Text; got != text {
			t.Errorf("Fallback(%q).Text = %q, want %q", q, got, text)
		}
	}
}

// The fallback hashes the text as it is: generated cursor and temp-table
// names aren't collapsed, since a regex can't find each name in arbitrary
// SQL without lexing it. Each name gets its own fallback fingerprint.
func TestFallbackKeepsGeneratedNames(t *testing.T) {
	for _, pair := range [][2]string{
		{"fetch 10 from users_cursor_ab12 returning with (old as o) o.id", "fetch 10 from users_cursor_zz99 returning with (old as o) o.id"},
		{"update users_temp_table_qwerty set x = 1 returning with (old as o) o.x", "update users_temp_table_asdfgh set x = 1 returning with (old as o) o.x"},
		// Compact SQL, where a greedy pattern would only see the last name.
		{"select * from users_temp_table_abcdef,users_temp_table_ghijkl", "select * from users_temp_table_zzzzzz,users_temp_table_ghijkl"},
	} {
		a, b := Fallback(pair[0]), Fallback(pair[1])
		if a.Fingerprint == b.Fingerprint {
			t.Errorf("%q and %q share %s", pair[0], pair[1], a.Fingerprint)
		}
		if a.Text != pair[0] {
			t.Errorf("Text = %q, want %q", a.Text, pair[0])
		}
	}
}

var pgQueryFingerprint = regexp.MustCompile(`^[0-9a-f]{16}$`)

// pg_query fingerprints are 16 lowercase hex digits. A fallback starts with
// "unparsed-", which no pg_query fingerprint can, so the two never collide.
func TestFallbackNeverCollidesWithParsedFingerprints(t *testing.T) {
	n := 0
	for _, c := range loadCorpus(t) {
		fp, err := Normalized(c.query, Options{})
		if err != nil {
			continue
		}
		n++
		if !pgQueryFingerprint.MatchString(fp) {
			t.Errorf("%s: parsed fingerprint %q isn't 16 hex digits", c.name, fp)
		}
		if IsFallback(fp) {
			t.Errorf("%s: parsed fingerprint %q looks like a fallback", c.name, fp)
		}
		fb := Fallback(c.query).Fingerprint
		if !IsFallback(fb) || !regexp.MustCompile(`^unparsed-[0-9a-f]{16}$`).MatchString(fb) {
			t.Errorf("%s: fallback %q has the wrong shape", c.name, fb)
		}
	}
	if n < 50 {
		t.Fatalf("only %d corpus cases parsed; the check is too weak", n)
	}
}

func TestFallbackTextFitsTheServerLimit(t *testing.T) {
	long := "update t set x = 'é" + strings.Repeat("é", 10000) + "' returning with (old as o) o.x"
	got := Fallback(long)
	if len(got.Text) > MaxFallbackTextBytes || !utf8.ValidString(got.Text) {
		t.Fatalf("Text is %d bytes (valid UTF-8 %v), want at most %d", len(got.Text), utf8.ValidString(got.Text), MaxFallbackTextBytes)
	}
	if !strings.HasPrefix(long, got.Text) {
		t.Fatal("Text should be a prefix of the statement")
	}
	other := Fallback(long[:len(long)-1] + "y")
	if other.Fingerprint == got.Fingerprint {
		t.Fatal("the fingerprint should hash the whole statement, not just the stored prefix")
	}
}

func TestNormalizedMarksParserRejection(t *testing.T) {
	_, err := Normalized(pg18Update, Options{})
	if !errors.Is(err, ErrParse) {
		t.Fatalf("Normalized(%q) error = %v, want ErrParse", pg18Update, err)
	}
	if err.Error() != "failed to parse" {
		t.Fatalf("error text changed to %q; the golden file pins it", err)
	}
	if _, err := Normalized("select 1", Options{}); err != nil {
		t.Fatal(err)
	}
}
