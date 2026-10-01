// Package migrate applies the embedded goose migrations to the rotten
// database, then reapplies migrations/permissions.sql.
package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
	"github.com/pressly/goose/v3"

	"github.com/benchub/rotten/migrations"
)

// VersionTable is where goose records applied migrations.
const VersionTable = "public.goose_db_version"

// Up connects with dsn, which should log in as rotten_owner, applies every
// pending migration, and then reapplies permissions.sql. It returns the
// versions it applied, in order, so a database that's already current
// returns none.
//
// The roles permissions.sql grants to (rotten_ingest, rotten_ui, and
// rotten_readonly) must already exist. An operator creates them; migrate
// doesn't, so rotten_owner never needs CREATEROLE.
func Up(ctx context.Context, dsn string) ([]int64, error) {
	return UpWith(ctx, dsn, migrations.FS, migrations.Permissions)
}

// UpWith is Up with the migrations and the permissions SQL supplied by the
// caller. Tests use it to add a migration or a grant.
func UpWith(ctx context.Context, dsn string, migrationFS fs.FS, permissions string) ([]int64, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("migrate: open: %w", err)
	}
	defer db.Close()
	// Qualify the version table. Unqualified, goose looks for it only in
	// current_schema(), which becomes rotten once 0001 sets the database's
	// search_path, so a second run wouldn't find it.
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrationFS,
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
	if err := applyPermissions(ctx, db, permissions); err != nil {
		return applied, err
	}
	return applied, nil
}

// applyPermissions runs permissions in one transaction, so a failure leaves
// the previous grants in place.
func applyPermissions(ctx context.Context, db *sql.DB, permissions string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("migrate: permissions: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // a no-op after Commit
	// With no arguments, pgx sends this over the simple protocol, so the
	// file's many statements run in one round trip.
	if _, err := tx.ExecContext(ctx, permissions); err != nil {
		return fmt.Errorf("migrate: permissions.sql: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("migrate: permissions: commit: %w", err)
	}
	return nil
}
