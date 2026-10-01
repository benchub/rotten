// Package migrate applies the embedded goose migrations to the rotten
// database.
package migrate

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
	"github.com/pressly/goose/v3"

	"github.com/benchub/rotten/migrations"
)

// VersionTable is where goose records applied migrations.
const VersionTable = "public.goose_db_version"

// Up connects with dsn, which should log in as rotten_owner, and applies
// every pending migration. It returns the versions it applied, in order, so
// a database that's already current returns none.
func Up(ctx context.Context, dsn string) ([]int64, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("migrate: open: %w", err)
	}
	defer db.Close()
	// Qualify the version table. Unqualified, goose looks for it only in
	// current_schema(), which becomes rotten once 0001 sets the database's
	// search_path, so a second run wouldn't find it.
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS,
		goose.WithTableName(VersionTable))
	if err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	results, err := p.Up(ctx)
	var applied []int64
	for _, r := range results {
		applied = append(applied, r.Source.Version)
	}
	if err != nil {
		return applied, fmt.Errorf("migrate: %w", err)
	}
	return applied, nil
}
