package main

import (
	"database/sql"
	"errors"
	"log"
	"reflect"
	"regexp"

	reflectwalk "github.com/mitchellh/reflectwalk"
	pg_query "github.com/pganalyze/pg_query_go/v2"
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

// replace all cursors in the parse tree with a constant cursor
var cursorRE, _ = regexp.Compile(`^([^\s]+)[_\-]cursor_[0-9a-z]+([^\s]*)$`)

// replace all temp tables in the parse tree with a constant temp table name
var tempTableRE, _ = regexp.Compile(`([^\s]+)_temp_table_[0-9a-z]{6}[0-9a-z]*([^\s]*)`)

// Some helper functions for reflectwalk to traverse the protobuf-derived parse tree of a query
type walker struct {
	depth int
}

func (s *walker) Struct(v reflect.Value) error {
	return nil
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
		v.SetString(tempTableRE.ReplaceAllString(v.String(), "${1}_temp_table_x${2}"))
	case "Portalname":
		v.SetString(cursorRE.ReplaceAllString(v.String(), "${1}_cursor_x${2}"))
	case "Schemaname":
		if len(v.String()) > 0 {
			v.SetString("some_schema")
		}
	default:
		// Most fields aren't going to hold cursor or temp table identifiers, but
		// we don't have the energy to make an exhaustive list of where they might show up,
		// and it's not *terrible* (at least in our case) to just try everywhere we can.
		if v.CanSet() && v.Kind() == reflect.String {
			v.SetString(cursorRE.ReplaceAllString(v.String(), "${1}_cursor_x${2}"))
			v.SetString(tempTableRE.ReplaceAllString(v.String(), "${1}_temp_table_x${2}"))
		}
	}

	if skipIt {
		return reflectwalk.SkipEntry
	}

	return nil
}

// Take a query, normalize some elements to keep the "same" query from having different
// fingerprints, and return a short fingerprint of the query as determined by postgres'
// fingerprint logic.
// e.g. "SELECT 1" -> "02a281c251c3a43d2fe7457dff01f76c5cc523f8c8"
func normalized_fingerprint(event *QueryEvent) (fingerprint string, err error) {
	/* This logic is a noble cause but I don't think it's robust enough for prime time
	    modified_query := inRE.ReplaceAllString(
			valuesRE.ReplaceAllString(
				event.query,
				"VALUES ${1}"),
			"IN (1)")*/
	modified_query := event.query

	tree, err := pg_query.Parse(modified_query)
	if err != nil {
		log.Println("couldn't parse query", modified_query, err)
		return "", errors.New("failed to parse")
	}

	// Now that we have our query tree, munge it to normalize queries as defined in our StructField walker above
	for _, statement := range tree.Stmts {
		var w = &walker{depth: 0}
		err := reflectwalk.Walk(statement.Stmt, w)
		if err != nil {
			log.Println("couldn't walk tree", modified_query, reflect.ValueOf(statement.Stmt), err)
			return "", errors.New("failed to walk tree")
		}
	}

	// Turn our munged tree back into a query
	deparsed, err := pg_query.Deparse(tree)
	if err != nil {
		// we can't seem to use our golang parse tree, so let's just see if we can't fingerprint it straight.
		// This might end up in a lot of fingerprints that are only different based on their schema name,
		// but it's the best we can do.
		if cursorRE.MatchString(event.query) || tempTableRE.MatchString(event.query) {
			// EXCEPT - if the query matches our cursor or temp table RE, that's just going to grow as a function of usage, not of schema count.
			// So actually _don't_ fingerprint something that matches either of those regexes.
			log.Println("couldn't fingerprint non-deparsable query involving cursors or temp tables: ", modified_query, err)
			return "", errors.New("failed to deparse; no fingerprint fallback")
		} else {
			fingerprint, err = pg_query.Fingerprint(modified_query)
			if err != nil {
				log.Println("couldn't fingerprint non-deparsable query: ", modified_query, err)
				return "", errors.New("failed to deparse and fingerprint fallback")
			} else {
				return fingerprint, nil
			}
		}
	}

	fingerprint, err = pg_query.Fingerprint(deparsed)
	if err != nil {
		log.Println("couldn't fingerprint deparsed query:", modified_query, ", deparsed:", deparsed, ", error:", err)
		return "", errors.New("failed to fingerprint depared query")
	}

	return fingerprint, nil
}

// Find the sequence id of this fingerprint in the rotten db.
func normalized_fingerprint_id(rottenDB *sql.DB, fingerprint string, event *QueryEvent) (db_id uint64, err error) {
	var fingerprint_id uint64

	normalized, err := pg_query.Normalize(event.query)
	if err != nil {
		log.Println("couldn't normalize query", event.query, err)
		return 0, errors.New("failed to normalize")
	}

	row := rottenDB.QueryRow(`select id from fingerprints where fingerprint=$1`, fingerprint)
	if err := row.Scan(&fingerprint_id); err == nil {
		// yay, we have our ID
	} else if err == sql.ErrNoRows {
		if err := rottenDB.QueryRow(`insert into fingerprints(fingerprint,normalized) values ($1,$2) returning id`, fingerprint, normalized).Scan(&fingerprint_id); err == nil {
			// yay, we have our ID
		} else {
			// we couldn't insert, probably because another session got here first. See what id it got
			if err := rottenDB.QueryRow(`select id from fingerprints where fingerprint=$1`, fingerprint).Scan(&fingerprint_id); err == nil {
				// yay, we have our ID
			} else {
				log.Fatalln("couldn't select newly inserted fingerprint", err)
				// will now exit because Fatal
			}
		}
	} else {
		log.Fatalln("couldn't select fingerprint", fingerprint, err)
		// will now exit because Fatal
	}

	return fingerprint_id, nil
}
