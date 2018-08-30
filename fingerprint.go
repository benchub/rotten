package main

import (
  "log"
  "fmt"
  "errors"
  "regexp"
  "crypto/sha1"
  "database/sql"
  "github.com/lfittl/pg_query_go"
)

var schmeaRE,_ = regexp.Compile(`"schemaname": "[^"]+"`)

func normalized_fingerprint(event *QueryEvent) (fingerprint string, err error) {
  tree, err := pg_query.ParseToJSON(event.query)
  if err != nil {
    log.Printf("couldn't parse query to JSON", event.query, err)
    return "", errors.New("failed to JSONify")
  }

  h := sha1.New()
  h.Write([]byte(schmeaRE.ReplaceAllString(tree,"\"schemaname\": \"x\"")))
  bs := h.Sum(nil)

  fingerprint = fmt.Sprintf("%x", bs)

  return fingerprint, nil
}


func normalized_fingerprint_id(rottenDB *sql.DB, fingerprint string, event *QueryEvent) (db_id uint64, err error) {
  var fingerprint_id uint64

  normalized, err := pg_query.Normalize(event.query)
  if err != nil {
    log.Printf("couldn't normalize query", event.query, err)
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
