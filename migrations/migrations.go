// Package migrations embeds the goose SQL migrations for the rotten database,
// plus permissions.sql, which migrate reapplies after every run.
package migrations

import "embed"

// FS holds every migration file. Migration files start with a digit, which
// keeps permissions.sql out of goose's sight.
//
//go:embed [0-9]*.sql
var FS embed.FS

// Permissions is permissions.sql: it revokes everything from the
// application roles, then grants what each one needs, table by table.
//
//go:embed permissions.sql
var Permissions string
