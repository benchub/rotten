package fingerprint

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"regexp"

	reflectwalk "github.com/mitchellh/reflectwalk"
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// replace all IN clauses with a single entry
// e.g. IN (1,2,3) becomes IN (1)
var inRE, _ = regexp.Compile(`(?i)in\W*\([\d'][^)]*\)`)

// replace all VALUES clauses with a VALUES statement only having the first tuple value
// e.g. VALUES (1,2),(3,4),(5,6) becomes VALUES (1,2)
var valuesRE, _ = regexp.Compile(`(?i)values\W*(\([^,\)]+(,[^,\)]+)*\))(,(\([^,\)]+(,[^,\)]+)*\)))*`)

// replace random index names that repack might generate with a constant name
var randomIndexRE, _ = regexp.Compile(`^index_\d+$`)

// replace random table names that repack might generate with a constant name
var randomTableRE, _ = regexp.Compile(`^table_\d+$`)

// DefaultCursorPattern matches generated cursor names, like
// users_cursor_ab12. It's unanchored. The walker anchors it to whole
// identifiers, and the deparse fallback uses it as is to find a cursor name
// anywhere in a statement. Group 1 is the prefix and group 2 the suffix kept
// around the collapsed name.
const DefaultCursorPattern = `([^\s]+)[_\-]cursor_[0-9a-z]+([^\s]*)`

// DefaultTempTablePattern matches generated temp-table names with a random
// suffix of six or more characters, like users_temp_table_qwerty. Groups work
// like DefaultCursorPattern's.
const DefaultTempTablePattern = `([^\s]+)_temp_table_[0-9a-z]{6}[0-9a-z]*([^\s]*)`

var defaultPatterns = mustPatterns(DefaultCursorPattern, DefaultTempTablePattern)

// patterns holds the compiled cursor and temp-table regexes.
type patterns struct {
	cursor        *regexp.Regexp // anchored, for single identifiers in the tree
	cursorInQuery *regexp.Regexp // unanchored, for whole statements
	tempTable     *regexp.Regexp
}

func compilePatterns(cursor, tempTable string) (*patterns, error) {
	if cursor == "" {
		cursor = DefaultCursorPattern
	}
	if tempTable == "" {
		tempTable = DefaultTempTablePattern
	}
	var p patterns
	var err error
	if p.cursorInQuery, err = compileGroups("CursorPattern", cursor); err != nil {
		return nil, err
	}
	if p.cursor, err = regexp.Compile(`^(?:` + cursor + `)$`); err != nil {
		return nil, fmt.Errorf("CursorPattern %q: %w", cursor, err)
	}
	if p.tempTable, err = compileGroups("TempTablePattern", tempTable); err != nil {
		return nil, err
	}
	return &p, nil
}

func compileGroups(name, pattern string) (*regexp.Regexp, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("%s %q is not a valid regex: %w", name, pattern, err)
	}
	if re.NumSubexp() != 2 {
		return nil, fmt.Errorf("%s %q needs exactly two capture groups (prefix and suffix), found %d; write any other grouping as (?:...)", name, pattern, re.NumSubexp())
	}
	if re.MatchString("") {
		return nil, fmt.Errorf("%s %q matches the empty string, so it would rewrite every name", name, pattern)
	}
	return re, nil
}

func mustPatterns(cursor, tempTable string) *patterns {
	p, err := compilePatterns(cursor, tempTable)
	if err != nil {
		panic(err)
	}
	return p
}

// Options control how Normalized groups queries. The zero value is the
// default behavior. Build non-default patterns with NewOptions.
type Options struct {
	// KeepSchemas turns schema collapsing off. By default, every table
	// reference, qualified or not, gets the same placeholder schema, so
	// users, public.users, and shard_1.users share a fingerprint. With
	// KeepSchemas, schema names are left as written.
	KeepSchemas bool

	// patterns is nil for the defaults.
	patterns *patterns
}

// NewOptions builds Options with the given cursor and temp-table patterns.
// An empty pattern means the default. It returns an error if a pattern
// doesn't compile or has fewer than two capture groups.
func NewOptions(keepSchemas bool, cursorPattern, tempTablePattern string) (Options, error) {
	p, err := compilePatterns(cursorPattern, tempTablePattern)
	if err != nil {
		return Options{}, err
	}
	return Options{KeepSchemas: keepSchemas, patterns: p}, nil
}

func (o Options) pats() *patterns {
	if o.patterns == nil {
		return defaultPatterns
	}
	return o.patterns
}

// Some helper functions for reflectwalk to traverse the protobuf-derived parse tree of a query
type walker struct {
	depth int
	opts  Options
	pats  *patterns
}

var rangeVarType = reflect.TypeOf(pg_query.RangeVar{})

func (s *walker) Struct(v reflect.Value) error {
	// Collapse schemas on table references (RangeVar) only, qualified or
	// not, so bare users matches shard_1.users. StructField collapses
	// other non-empty Schemaname fields, like CREATE SCHEMA's.
	if !s.opts.KeepSchemas && v.Type() == rangeVarType {
		if f := v.FieldByName("Schemaname"); f.CanSet() {
			f.SetString("some_schema")
		}
	}
	if v.CanAddr() {
		switch n := v.Addr().Interface().(type) {
		case *pg_query.A_Expr:
			rewriteArrayToIn(n)
		case *pg_query.ColumnRef:
			if !s.opts.KeepSchemas {
				n.Fields = collapseColumnRefFields(n.Fields)
			}
		case *pg_query.FuncCall:
			if !s.opts.KeepSchemas {
				n.Funcname = collapseFuncCallName(n.Funcname, n.Funcformat)
			}
		case *pg_query.TypeName:
			if !s.opts.KeepSchemas {
				n.Names = collapseTypeName(n.Names, n.PctType)
			}
		case *pg_query.DropStmt:
			if !s.opts.KeepSchemas {
				collapseDropObjects(n.RemoveType, n.Objects)
			}
		case *pg_query.CommentStmt:
			if !s.opts.KeepSchemas {
				collapseObjectNode(n.Objtype, n.Object)
			}
		case *pg_query.RenameStmt:
			if !s.opts.KeepSchemas {
				collapseObjectNode(n.RenameType, n.Object)
			}
		case *pg_query.AlterObjectDependsStmt:
			if !s.opts.KeepSchemas {
				collapseObjectNode(n.ObjectType, n.Object)
			}
		case *pg_query.AlterObjectSchemaStmt:
			if !s.opts.KeepSchemas {
				if n.Newschema != "" {
					n.Newschema = "some_schema"
				}
				collapseObjectNode(n.ObjectType, n.Object)
			}
		case *pg_query.AlterOwnerStmt:
			if !s.opts.KeepSchemas {
				collapseObjectNode(n.ObjectType, n.Object)
			}
		case *pg_query.SubLink:
			// x = ANY (subquery) and x IN (subquery) are the same
			// ANY_SUBLINK; IN just leaves the operator name out.
			if n.SubLinkType == pg_query.SubLinkType_ANY_SUBLINK && isOp(n.OperName, "=") {
				n.OperName = nil
			}
		}
	}
	return nil
}

// rewriteArrayToIn turns x = ANY(ARRAY[a, b]) into x IN (a, b), and
// x <> ALL(ARRAY[a, b]) into x NOT IN (a, b). Postgres parses IN lists into
// those same array forms, and Postgres 18 gives them one queryid. The new
// list reuses the array's element nodes, so the walker's other rewrites
// reach them whether it visits this struct before or after its fields.
//
// Two merges are accepted on purpose, since the forms are rare and close:
// a row-constructor element, x = ANY(ARRAY[(1, 2)]), groups with the row IN
// list x IN ((1, 2)), and a scalar-subquery element,
// x = ANY(ARRAY[(SELECT ...)]), becomes x IN ((SELECT ...)), which
// fingerprints like x IN (subquery) even though the scalar subquery errors
// on more than one row.
//
// A cast array, x = ANY(ARRAY[a, b]::T[]), becomes x IN (a::T, b::T).
// For a base type T, Postgres resolves ARRAY[...]::T[] by coercing each
// element to T, so this is the same query, and pg_stat_statements on 16 and
// 18 gives the two forms one queryid. A domain array (::posint[]) isn't
// exact: Postgres coerces it as a whole, not per element. Merging it anyway
// is an accepted over-merge, like IN (1::bigint) below. The cast can change the element type and the operator
// (::bigint[] on an int column picks int4 = int8), and Postgres keeps that
// apart from the uncast IN list. The fingerprint doesn't: it already ignores
// element casts in IN lists, so IN (1::bigint) and IN (1) share one. That's
// the accepted cost, and the cast array now follows the IN list's rule.
//
// It leaves alone: other operators (< ANY, = ALL, <> ANY), schema-qualified
// OPERATOR(...) syntax, a non-literal array (= ANY($1) already fingerprints
// like = $1, and '{1,2}'::int[] is a constant), a cast to anything but a
// one-dimensional array type, an empty array (IN () isn't valid SQL), and
// multidimensional arrays (IN ((1, 2)) would mean a row comparison).
func rewriteArrayToIn(e *pg_query.A_Expr) {
	var op string
	switch {
	case e.Kind == pg_query.A_Expr_Kind_AEXPR_OP_ANY && isOp(e.Name, "="):
		op = "="
	case e.Kind == pg_query.A_Expr_Kind_AEXPR_OP_ALL && isOp(e.Name, "<>"):
		op = "<>"
	default:
		return
	}
	arr := e.Rexpr.GetAArrayExpr()
	var elemType *pg_query.TypeName
	if tc := e.Rexpr.GetTypeCast(); tc != nil {
		if elemType = arrayElemType(tc.TypeName); elemType == nil {
			return
		}
		arr = tc.Arg.GetAArrayExpr()
	}
	if arr == nil || len(arr.Elements) == 0 {
		return
	}
	for _, el := range arr.Elements {
		if el.GetAArrayExpr() != nil {
			return
		}
	}
	elems := arr.Elements
	if elemType != nil {
		elems = make([]*pg_query.Node, len(arr.Elements))
		for i, el := range arr.Elements {
			elems[i] = &pg_query.Node{Node: &pg_query.Node_TypeCast{TypeCast: &pg_query.TypeCast{
				Arg:      el,
				TypeName: copyTypeName(elemType),
				Location: -1,
			}}}
		}
	}
	e.Kind = pg_query.A_Expr_Kind_AEXPR_IN
	e.Name = []*pg_query.Node{pg_query.MakeStrNode(op)}
	e.Rexpr = pg_query.MakeListNode(elems)
}

// arrayElemType returns the element type of a one-dimensional array cast,
// like int[] or bigint ARRAY, or nil for anything else. Postgres treats
// int[][] like int[], but that's rare enough to leave alone.
func arrayElemType(tn *pg_query.TypeName) *pg_query.TypeName {
	if tn == nil || len(tn.ArrayBounds) != 1 || tn.Setof || tn.PctType {
		return nil
	}
	el := copyTypeName(tn)
	el.ArrayBounds = nil
	return el
}

// copyTypeName copies tn's fields into a new TypeName, so each pushed-down
// cast gets its own node. The name and typmod nodes are shared, which is
// fine since nothing rewrites them.
func copyTypeName(tn *pg_query.TypeName) *pg_query.TypeName {
	return &pg_query.TypeName{
		Names:       tn.Names,
		TypeOid:     tn.TypeOid,
		Setof:       tn.Setof,
		PctType:     tn.PctType,
		Typmods:     tn.Typmods,
		Typemod:     tn.Typemod,
		ArrayBounds: tn.ArrayBounds,
		Location:    -1,
	}
}

// isOp reports whether name is the single unqualified operator op.
func isOp(name []*pg_query.Node, op string) bool {
	return len(name) == 1 && name[0].GetString_() != nil && name[0].GetString_().Sval == op
}

func collapseColumnRefFields(fields []*pg_query.Node) []*pg_query.Node {
	switch len(fields) {
	case 3:
		if isStringNode(fields[0]) {
			return append(fields[:0:0], fields[1:]...)
		}
	case 4:
		if isStringNode(fields[1]) {
			out := append(fields[:0:0], fields[0])
			return append(out, fields[2:]...)
		}
	}
	return fields
}

func collapseQualifiedObjectName(name []*pg_query.Node) []*pg_query.Node {
	switch len(name) {
	case 2:
		if isStringNode(name[0]) {
			return append(name[:0:0], name[1:]...)
		}
	case 3:
		if isStringNode(name[1]) {
			out := append(name[:0:0], name[0])
			return append(out, name[2:]...)
		}
	}
	return name
}

func collapseFuncCallName(name []*pg_query.Node, format pg_query.CoercionForm) []*pg_query.Node {
	if format == pg_query.CoercionForm_COERCE_SQL_SYNTAX {
		return name
	}
	switch len(name) {
	case 2:
		if stringNodeValue(name[0]) == "pg_catalog" {
			return name
		}
	case 3:
		if stringNodeValue(name[1]) == "pg_catalog" {
			return name
		}
	}
	return collapseQualifiedObjectName(name)
}

func collapseTypeName(name []*pg_query.Node, pctType bool) []*pg_query.Node {
	if pctType {
		return collapseColumnRefFields(name)
	}
	switch len(name) {
	case 2:
		if stringNodeValue(name[0]) == "pg_catalog" {
			return name
		}
	case 3:
		if stringNodeValue(name[1]) == "pg_catalog" {
			return name
		}
	}
	return collapseQualifiedObjectName(name)
}

func collapseDropObjects(objtype pg_query.ObjectType, objects []*pg_query.Node) {
	for _, obj := range objects {
		collapseObjectNode(objtype, obj)
	}
}

func collapseObjectNode(objtype pg_query.ObjectType, obj *pg_query.Node) {
	if obj == nil {
		return
	}
	if list := obj.GetList(); list != nil {
		list.Items = collapseObjectNameItems(objtype, list.Items)
		return
	}
	if withArgs := obj.GetObjectWithArgs(); withArgs != nil {
		withArgs.Objname = collapseObjectNameItems(objtype, withArgs.Objname)
	}
}

func collapseObjectNameItems(objtype pg_query.ObjectType, items []*pg_query.Node) []*pg_query.Node {
	switch objectNameShape(objtype) {
	case objectNameAny:
		return collapseQualifiedObjectName(items)
	case objectNameTableMember:
		if len(items) < 3 {
			return items
		}
		table := collapseQualifiedObjectName(items[:len(items)-1])
		out := make([]*pg_query.Node, 0, len(table)+1)
		out = append(out, table...)
		return append(out, items[len(items)-1])
	case objectNameAccessMethodMember:
		if len(items) < 3 {
			return items
		}
		name := collapseQualifiedObjectName(items[1:])
		out := make([]*pg_query.Node, 0, len(name)+1)
		out = append(out, items[0])
		return append(out, name...)
	default:
		return items
	}
}

type objectNameKind int

const (
	objectNameNone objectNameKind = iota
	objectNameAny
	objectNameTableMember
	objectNameAccessMethodMember
)

func objectNameShape(objtype pg_query.ObjectType) objectNameKind {
	switch objtype {
	case pg_query.ObjectType_OBJECT_AGGREGATE,
		pg_query.ObjectType_OBJECT_COLLATION,
		pg_query.ObjectType_OBJECT_CONVERSION,
		pg_query.ObjectType_OBJECT_DOMAIN,
		pg_query.ObjectType_OBJECT_FOREIGN_TABLE,
		pg_query.ObjectType_OBJECT_FUNCTION,
		pg_query.ObjectType_OBJECT_INDEX,
		pg_query.ObjectType_OBJECT_MATVIEW,
		pg_query.ObjectType_OBJECT_PROCEDURE,
		pg_query.ObjectType_OBJECT_ROUTINE,
		pg_query.ObjectType_OBJECT_SEQUENCE,
		pg_query.ObjectType_OBJECT_STATISTIC_EXT,
		pg_query.ObjectType_OBJECT_TABLE,
		pg_query.ObjectType_OBJECT_TSCONFIGURATION,
		pg_query.ObjectType_OBJECT_TSDICTIONARY,
		pg_query.ObjectType_OBJECT_TSPARSER,
		pg_query.ObjectType_OBJECT_TSTEMPLATE,
		pg_query.ObjectType_OBJECT_TYPE,
		pg_query.ObjectType_OBJECT_VIEW:
		return objectNameAny
	case pg_query.ObjectType_OBJECT_COLUMN,
		pg_query.ObjectType_OBJECT_POLICY,
		pg_query.ObjectType_OBJECT_RULE,
		pg_query.ObjectType_OBJECT_TABCONSTRAINT,
		pg_query.ObjectType_OBJECT_TRIGGER:
		return objectNameTableMember
	case pg_query.ObjectType_OBJECT_OPCLASS,
		pg_query.ObjectType_OBJECT_OPFAMILY:
		return objectNameAccessMethodMember
	default:
		return objectNameNone
	}
}

func isStringNode(n *pg_query.Node) bool {
	return n != nil && n.GetString_() != nil
}

func stringNodeValue(n *pg_query.Node) string {
	if str := n.GetString_(); str != nil {
		return str.Sval
	}
	return ""
}

func (s *walker) StructField(f reflect.StructField, v reflect.Value) error {
	// Skip over all the protobuf fields we couldn't care less about
	// Modify the things we do want to change
	var skipIt = false
	switch f.Name {
	case "sizeCache":
		skipIt = true
	case "state":
		skipIt = true
	case "unknownFields":
		skipIt = true
	case "DoNotCompare":
		skipIt = true
	case "DoNotCopy":
		skipIt = true
	case "atomicMessageInfo":
		skipIt = true
	case "NoUnkeyedLiterals":
		skipIt = true
	case "Rolename":
		v.SetString("some_role")
	case "HowMany":
		v.SetInt(0)
	case "Idxname":
		v.SetString(randomIndexRE.ReplaceAllString(v.String(), "some_index"))
	case "Relname":
		v.SetString(randomTableRE.ReplaceAllString(v.String(), "some_table"))
		v.SetString(s.pats.tempTable.ReplaceAllString(v.String(), "${1}_temp_table_x${2}"))
	case "Portalname":
		v.SetString(s.pats.cursor.ReplaceAllString(v.String(), "${1}_cursor_x${2}"))
	case "Schemaname":
		// Struct also fills in empty RangeVar schemas. Here, collapse any
		// non-empty Schemaname, like CREATE SCHEMA's.
		if !s.opts.KeepSchemas && len(v.String()) > 0 {
			v.SetString("some_schema")
		}
	default:
		// Most fields aren't going to hold cursor or temp table identifiers, but
		// we don't have the energy to make an exhaustive list of where they might show up,
		// and it's not *terrible* (at least in our case) to just try everywhere we can.
		if v.CanSet() && v.Kind() == reflect.String {
			v.SetString(s.pats.cursor.ReplaceAllString(v.String(), "${1}_cursor_x${2}"))
			v.SetString(s.pats.tempTable.ReplaceAllString(v.String(), "${1}_temp_table_x${2}"))
		}
	}

	if skipIt {
		return reflectwalk.SkipEntry
	}

	return nil
}

// Normalized takes a query, normalizes some elements to keep the "same" query from having different
// fingerprints, and returns a short fingerprint of the query as determined by postgres'
// fingerprint logic.
// e.g. "SELECT 1" -> "50fde20626009aba"
func Normalized(query string, opts Options) (fingerprint string, err error) {
	/* This logic is a noble cause but I don't think it's robust enough for prime time
	    modified_query := inRE.ReplaceAllString(
			valuesRE.ReplaceAllString(
				query,
				"VALUES ${1}"),
			"IN (1)")*/
	modified_query := query

	tree, err := pg_query.Parse(modified_query)
	if err != nil {
		log.Println("couldn't parse query", modified_query, err)
		return "", errors.New("failed to parse")
	}

	// Now that we have our query tree, munge it to normalize queries as defined in our StructField walker above
	for _, statement := range tree.Stmts {
		var w = &walker{depth: 0, opts: opts, pats: opts.pats()}
		err := reflectwalk.Walk(statement.Stmt, w)
		if err != nil {
			log.Println("couldn't walk tree", modified_query, reflect.ValueOf(statement.Stmt), err)
			return "", errors.New("failed to walk tree")
		}
	}

	// Turn our munged tree back into a query
	deparsed, err := pg_query.Deparse(tree)
	if err != nil {
		return deparseFallback(modified_query, opts.pats(), err)
	}

	fingerprint, err = pg_query.Fingerprint(deparsed)
	if err != nil {
		log.Println("couldn't fingerprint deparsed query:", modified_query, ", deparsed:", deparsed, ", error:", err)
		return "", errors.New("failed to fingerprint deparsed query")
	}

	return fingerprint, nil
}

// Query returns pg_query's normalized form of query for storage with a
// fingerprint. The ingest server stores this representative text when it
// first sees a fingerprint.
func Query(query string) (string, error) {
	normalized, err := pg_query.Normalize(query)
	if err != nil {
		log.Println("couldn't normalize query", query, err)
		return "", errors.New("failed to normalize")
	}
	return normalized, nil
}

// deparseFallback handles a query whose munged parse tree couldn't be
// deparsed. deparseErr is the Deparse error, used only for logging.
func deparseFallback(query string, pats *patterns, deparseErr error) (string, error) {
	// we can't seem to use our golang parse tree, so let's just see if we can't fingerprint it straight.
	// This might end up in a lot of fingerprints that are only different based on their schema name,
	// but it's the best we can do.
	if pats.cursorInQuery.MatchString(query) || pats.tempTable.MatchString(query) {
		// EXCEPT - if the query matches our cursor or temp table RE, that's just going to grow as a function of usage, not of schema count.
		// So actually _don't_ fingerprint something that matches either of those regexes.
		log.Println("couldn't fingerprint non-deparsable query involving cursors or temp tables: ", query, deparseErr)
		return "", errors.New("failed to deparse; no fingerprint fallback")
	}
	fingerprint, err := pg_query.Fingerprint(query)
	if err != nil {
		log.Println("couldn't fingerprint non-deparsable query: ", query, err)
		return "", errors.New("failed to deparse and fingerprint fallback")
	}
	return fingerprint, nil
}
