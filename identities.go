package main

import (
  "fmt"
  "log"
  "sync"
  "database/sql"
  "regexp"
)

var protectedActions = struct{
  sync.RWMutex
  m map[string]uint32
} {m: make(map[string]uint32)}

var protectedControllers = struct{
  sync.RWMutex
  m map[string]uint32
} {m: make(map[string]uint32)}

var re_controller,_ = regexp.Compile(`.+/\*controller:(.+),action:.+(,hostname:.*,pid:\d+)?,context_id:[\-0-9a-f]+\*/`)
var re_action,_ = regexp.Compile(`.+/\*controller:.+,action:(.+)(,hostname:.*,pid:\d+)?,context_id:[\-0-9a-f]+\*/`)



func find_controller_id(rottenDB *sql.DB, event *QueryEvent) (controller_qid string, controller_description string) {
  var db_id uint32

  if re_controller.MatchString(event.query) {
    matches := re_controller.FindStringSubmatch(event.query)
    if len(matches) > 1 {
      // We have a controller; have we seen it before?
      protectedControllers.Lock()
      existing, present := protectedControllers.m[matches[1]]
      if present {
        // oh hey, we've already seen this. Use it.
        protectedControllers.Unlock()

        db_id = existing
      } else {
        if err := rottenDB.QueryRow(`select id from controllers where controller=$1`,matches[1]).Scan(&db_id); err == nil {
          // yay, we have our ID

        } else if err == sql.ErrNoRows {
          if err := rottenDB.QueryRow(`insert into controllers(controller) values ($1) returning id`,matches[1]).Scan(&db_id); err == nil {
            // yay, we have our ID
          } else {
            // we couldn't insert, probably because another session got here first. See what id it got
            if err := rottenDB.QueryRow(`select id from controllers where controller=$1`,matches[1]).Scan(&db_id); err == nil {
              // yay, we have our ID
            } else {
               log.Fatalln("couldn't select newly inserted controller", err)
               // will now exit because Fatal
            }
          }
        } else {
          log.Fatalln("couldn't select from controllers", err)
          // will now exit because Fatal
        }

        protectedControllers.m[matches[1]] = db_id
        protectedControllers.Unlock()
      }

      controller_qid = fmt.Sprintf(",%d",db_id)
      controller_description = ",controller_id"
      return controller_qid, controller_description
    }
  }

  return "",""
}

func find_action_id(rottenDB *sql.DB, event *QueryEvent) (action_qid string, action_description string) {
  var db_id uint32

  if re_action.MatchString(event.query) {
    matches := re_action.FindStringSubmatch(event.query)
    if len(matches) > 1 {
      // We have a action; have we seen it before?
      protectedActions.Lock()
      existing, present := protectedActions.m[matches[1]]
      if present {
        // oh hey, we've already seen this. Use it.
        protectedActions.Unlock()

        db_id = existing
      } else {
        if err := rottenDB.QueryRow(`select id from actions where action=$1`,matches[1]).Scan(&db_id); err == nil {
          // yay, we have our ID

        } else if err == sql.ErrNoRows {
          if err := rottenDB.QueryRow(`insert into actions(action) values ($1) returning id`,matches[1]).Scan(&db_id); err == nil {
            // yay, we have our ID
          } else {
            // we couldn't insert, probably because another session got here first. See what id it got
            if err := rottenDB.QueryRow(`select id from actions where action=$1`,matches[1]).Scan(&db_id); err == nil {
              // yay, we have our ID
            } else {
               log.Fatalln("couldn't select newly inserted action", err)
               // will now exit because Fatal
            }
          }
        } else {
          log.Fatalln("couldn't select from actions", err)
          // will now exit because Fatal
        }

        protectedActions.m[matches[1]] = db_id
        protectedActions.Unlock()
      }

      action_qid = fmt.Sprintf(",%d",db_id)
      action_description = ",action_id"
      return action_qid, action_description
    }
  }

  return "",""
}
