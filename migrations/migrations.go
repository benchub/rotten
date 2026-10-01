// Package migrations embeds the goose SQL migrations for the rotten database.
package migrations

import "embed"

// FS holds every migration file.
//
//go:embed *.sql
var FS embed.FS
