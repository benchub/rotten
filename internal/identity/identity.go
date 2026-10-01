// Package identity maps the marginalia contexts in a query (controller,
// action, job tag) to their IDs in the rotten DB, caching what it has seen.
package identity

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tj/go-pg-escape"
)

// Cache maps one kind of identity's values to their rotten DB IDs. object is
// both the column name and, with an "s", the table name.
type Cache struct {
	object string

	mu sync.Mutex
	m  map[string]uint32
}

// NewCache returns an empty cache for object, such as "controller".
func NewCache(object string) *Cache {
	return &Cache{object: object, m: make(map[string]uint32)}
}

// Caches holds the three identity caches the worker uses.
type Caches struct {
	Controllers *Cache
	Actions     *Cache
	JobTags     *Cache
}

// NewCaches returns empty controller, action, and job tag caches.
func NewCaches() *Caches {
	return &Caches{
		Controllers: NewCache("controller"),
		Actions:     NewCache("action"),
		JobTags:     NewCache("job_tag"),
	}
}

// Find returns the ID for the last capture group re finds in query, looking
// it up in (or inserting it into) the rotten DB the first time it's seen. It
// returns 0 when re doesn't match or has no capture group.
func (list *Cache) Find(rottenDB *pgxpool.Pool, query string, re *regexp.Regexp) (db_id uint32) {
	object := list.object
	if re.MatchString(query) {
		matches := re.FindStringSubmatch(query)
		if len(matches) > 1 {
			// We have a match; have we seen it before?
			list.mu.Lock()
			existing, present := list.m[matches[len(matches)-1]]
			if present {
				// oh hey, we've already seen this. Use it.
				list.mu.Unlock()

				db_id = existing
			} else {
				s := escape.Escape("select id from %Is where %I=%L", object, object, matches[len(matches)-1])
				if err := rottenDB.QueryRow(context.Background(), s).Scan(&db_id); err == nil {
					// yay, we have our ID

				} else if err == pgx.ErrNoRows {
					i := escape.Escape("insert into %Is(%I) values (%L) returning id", object, object, matches[len(matches)-1])
					if err := rottenDB.QueryRow(context.Background(), i).Scan(&db_id); err == nil {
						// yay, we have our ID
					} else {
						// we couldn't insert, probably because another session got here first. See what id it got
						if err := rottenDB.QueryRow(context.Background(), s).Scan(&db_id); err == nil {
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
				list.mu.Unlock()
			}

			return db_id
		}
	}

	return 0
}

// CompileRegexes compiles the controller, action, and job regexes from
// the config, and reports which one failed to compile.
func CompileRegexes(controller, action, job string) (c, a, j *regexp.Regexp, err error) {
	if c, err = regexp.Compile(controller); err != nil {
		return nil, nil, nil, fmt.Errorf("compile ContextController: %w", err)
	}
	if a, err = regexp.Compile(action); err != nil {
		return nil, nil, nil, fmt.Errorf("compile ContextAction: %w", err)
	}
	if j, err = regexp.Compile(job); err != nil {
		return nil, nil, nil, fmt.Errorf("compile ContextJob: %w", err)
	}
	return c, a, j, nil
}
