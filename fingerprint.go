package main

import (
  "log"
  "strings"
  "bytes"
  "errors"
  "regexp"
  "database/sql"
  "os/exec"

  "github.com/lfittl/pg_query_go"
)

// replace all schemas in the parse tree with a constant
var schemaRE,_ = regexp.Compile(`"schemaname": "[^"]+"`)

// replace all cursors in the parse tree with a constant cursor
var cursorRE,_ = regexp.Compile(`([^\s]+)_cursor_[0-9a-z]{6}[0-9a-z]*([^\s]*)`)

// replace all temp tables in the parse tree with a constant temp table name
var tempTableRE,_ = regexp.Compile(`([^\s]+)_temp_table_[0-9a-z]{6}[0-9a-z]*([^\s]*)`)

// schema-qaulified columns do not get a schemaname parse object, which is dumb.
// However, they do get 3 columns, so we can at least work with that.
var qualifiedColumnsRE,_ = regexp.Compile(`{"ColumnRef": {"fields": \[{"String": {"str": "([^"]+)"}}, {"String": {"str": "([^"]+)"}}, {"String": {"str": "([^"]+)"}}\]`)


func normalized_fingerprint(event *QueryEvent) (fingerprint string, err error) {

  tree, err := pg_query.ParseToJSON(event.query)
  if err != nil {
    log.Println("couldn't parse query to JSON", event.query, err)
    return "", errors.New("failed to JSONify")
  }

  cmd := exec.Command("/usr/local/bin/deparse.rb")
  cmd.Stdin = strings.NewReader(cursorRE.ReplaceAllString(
                                  tempTableRE.ReplaceAllString(
                                    schemaRE.ReplaceAllString(
                                      qualifiedColumnsRE.ReplaceAllString(
                                        tree,
                                        "{\"ColumnRef\": {\"fields\": [{\"String\": {\"str\": \"x\"}}, {\"String\": {\"str\": \"$2\"}}, {\"String\": {\"str\": \"$3\"}}]"),
                                      "\"schemaname\": \"x\""),
                                    "${1}_temp_table_${2}"),
                                  "${1}_cursor_x${2}"))
  var out bytes.Buffer
  cmd.Stdout = &out
  err = cmd.Run()
  if err != nil {
    // we can't seem to use our json parse tree, so let's just see if we can't fingerprint it straight
    // This might end up in a lot of fingerprints that are only different based on their schema name,
    // but it's the best we can do.
    if cursorRE.MatchString(event.query) || tempTableRE.MatchString(event.query) {
      // EXCEPT - if the query matches our cursor or temp table RE, that's just going to grow as a function of usage, not of schema count.
      // So actually _don't_ fastfingerprint something that matches either of those regexes.
    } else {
      fingerprint, err = pg_query.FastFingerprint(event.query)
      if err != nil {
          log.Println("couldn't fingerprint non-deparsable query: ", event.query, err)
          return "", errors.New("failed to deparse or fingerprint")
      } else {
        return fingerprint, nil
      }
    }
  }

  _ = cmd.Wait()

  output := out.String()

  fingerprint, err = pg_query.FastFingerprint(output)
  if err != nil {
    // because pg_query has a bug in its deparsing code, if we have an update for multiple columns, they
    // are now missing a comma between them.
    // https://github.com/lfittl/pg_query/issues/86
//    log.Println("couldn't fingerprint query: ", event.query, "reparsed as:", output, ", because", err)
    return "", errors.New("failed to fingerprint")
  }

  return fingerprint, nil
}


func normalized_fingerprint_id(rottenDB *sql.DB, fingerprint string, event *QueryEvent) (db_id uint64, err error) {
  var fingerprint_id uint64

  normalized, err := pg_query.Normalize(event.query)
  if err != nil {
    log.Println("couldn't normalize query", event.query, err)
    return 0, errors.New("failed to normalize")
  }

  row := rottenDB.QueryRow(`select id from fingerprints where fingerprint=$1`,fingerprint)
  if err := row.Scan(&fingerprint_id); err == nil {
    // yay, we have our ID
  } else if err == sql.ErrNoRows {
    if err := rottenDB.QueryRow(`insert into fingerprints(fingerprint,normalized) values ($1,$2) returning id`,fingerprint,normalized).Scan(&fingerprint_id); err == nil {
      // yay, we have our ID
    } else {
      // we couldn't insert, probably because another session got here first. See what id it got
      if err := rottenDB.QueryRow(`select id from fingerprints where fingerprint=$1`,fingerprint).Scan(&fingerprint_id); err == nil {
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
