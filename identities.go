package main

import (
	"database/sql"
	"log"
	"regexp"
	"sync"

	"github.com/tj/go-pg-escape"
)

type ProtectedHash struct {
	sync.RWMutex
	m map[string]uint32
}

var protectedJobTags = ProtectedHash{m: make(map[string]uint32)}

var protectedActions = ProtectedHash{m: make(map[string]uint32)}

var protectedControllers = ProtectedHash{m: make(map[string]uint32)}

var re_controller, _ = regexp.Compile(`.+/\*.*controller(_with_namespace)?:([^,]+).*\*/`)
var re_action, _ = regexp.Compile(`.+/\*.*action:([^,]+).*\*/`)
var re_job_tag, _ = regexp.Compile(`.+/\*.*job_tag:([^,]+).*\*/`)

func find_job_tag_id(rottenDB *sql.DB, event *QueryEvent) (db_id uint32) {
	return find_identity(rottenDB, event, re_job_tag, &protectedJobTags, "job_tag")
}

func find_action_id(rottenDB *sql.DB, event *QueryEvent) (db_id uint32) {
	return find_identity(rottenDB, event, re_action, &protectedActions, "action")
}

func find_controller_id(rottenDB *sql.DB, event *QueryEvent) (db_id uint32) {
	return find_identity(rottenDB, event, re_controller, &protectedControllers, "controller")
}

func find_identity(rottenDB *sql.DB, event *QueryEvent, re *regexp.Regexp, list *ProtectedHash, object string) (db_id uint32) {
	if re.MatchString(event.query) {
		matches := re.FindStringSubmatch(event.query)
		if len(matches) > 1 {
			// We have a match; have we seen it before?
			list.Lock()
			existing, present := list.m[matches[len(matches)-1]]
			if present {
				// oh hey, we've already seen this. Use it.
				list.Unlock()

				db_id = existing
			} else {
				s := escape.Escape("select id from %Is where %I=%L", object, object, matches[len(matches)-1])
				if err := rottenDB.QueryRow(s).Scan(&db_id); err == nil {
					// yay, we have our ID

				} else if err == sql.ErrNoRows {
					i := escape.Escape("insert into %Is(%I) values (%L) returning id", object, object, matches[len(matches)-1])
					if err := rottenDB.QueryRow(i).Scan(&db_id); err == nil {
						// yay, we have our ID
					} else {
						// we couldn't insert, probably because another session got here first. See what id it got
						if err := rottenDB.QueryRow(s).Scan(&db_id); err == nil {
							// yay, we have our ID
						} else {
							log.Fatalln("couldn't select newly inserted", object, err)
							// will now exit because Fatal
						}
					}
				} else {
					log.Fatalln("couldn't select", object, err)
					// will now exit because Fatal
				}

				list.m[matches[len(matches)-1]] = db_id
				list.Unlock()
			}

			return db_id
		}
	}

	return 0
}
