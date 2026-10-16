package fingerprint

import (
	"regexp"
	"strings"
)

// savepointRE matches the savepoint statements Postgres 17+
// pg_stat_statements shows with the name replaced by $n, which the parser
// rejects. The groups are the kind: SAVEPOINT, RELEASE, or ROLLBACK TO.
var savepointRE = regexp.MustCompile(`(?is)^\s*(?:(savepoint)|(release)(?:\s+savepoint)?|(rollback)(?:\s+(?:work|transaction))?\s+to(?:\s+savepoint)?)\s+\$\d+[\s;]*$`)

// savepointName stands in for $n so the statement parses. pg_query's
// fingerprint ignores savepoint names, so the placeholder form groups with
// named savepoints of the same kind.
const savepointName = "some_savepoint"

// savepointPlaceholder returns query with its $n savepoint name replaced by
// savepointName, or false if query isn't one of those statements. Leading
// and trailing comments are ignored.
func savepointPlaceholder(query string) (string, bool) {
	m := savepointRE.FindStringSubmatch(stripOuterComments(query))
	switch {
	case m == nil:
		return "", false
	case m[1] != "":
		return "SAVEPOINT " + savepointName, true
	case m[2] != "":
		return "RELEASE SAVEPOINT " + savepointName, true
	default:
		return "ROLLBACK TO SAVEPOINT " + savepointName, true
	}
}

// canonicalSavepoint is the stored text for a savepointPlaceholder match.
func canonicalSavepoint(query string) string {
	named, _ := savepointPlaceholder(query)
	return strings.TrimSuffix(named, savepointName) + "$1"
}
