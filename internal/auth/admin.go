package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DB is what the admin functions need: a pgx conn or pool as rotten_owner.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Created is a new key. Token is the only copy of the secret.
type Created struct {
	ID    int64
	Token string
}

// CreateKey inserts a key and returns its token. fqdn "" leaves it unpinned;
// otherwise it must pass NormalizeFQDN and is stored normalized.
func CreateKey(ctx context.Context, db DB, name, fqdn, by string) (Created, error) {
	if name == "" {
		return Created{}, errors.New("key name is empty")
	}
	var f *string
	if fqdn != "" {
		n, err := NormalizeFQDN(fqdn)
		if err != nil {
			return Created{}, err
		}
		f = &n
	}
	secret, hash, err := NewSecret()
	if err != nil {
		return Created{}, err
	}
	var id int64
	err = db.QueryRow(ctx,
		`insert into rotten.api_keys (name, secret_hash, fqdn, created_by) values ($1, $2, $3, $4) returning id`,
		name, hash, f, by).Scan(&id)
	if err != nil {
		return Created{}, fmt.Errorf("create key %q: %w", name, err)
	}
	return Created{ID: id, Token: FormatKey(id, secret)}, nil
}

// RevokeKey revokes the live key named name.
func RevokeKey(ctx context.Context, db DB, name, by string) error {
	tag, err := db.Exec(ctx,
		`update rotten.api_keys set revoked_at = now(), revoked_by = $2 where name = $1 and revoked_at is null`,
		name, by)
	if err != nil {
		return fmt.Errorf("revoke key %q: %w", name, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("no live key named %q", name)
	}
	return nil
}

// Listed is a key as the CLI shows it. It has no secret or hash.
type Listed struct {
	ID         int64
	Name       string
	FQDN       *string
	CreatedAt  time.Time
	CreatedBy  string
	LastUsedAt *time.Time
	RevokedAt  *time.Time
	RevokedBy  *string
}

// ListKeys returns every key in id order.
func ListKeys(ctx context.Context, db DB) ([]Listed, error) {
	rows, err := db.Query(ctx,
		`select id, name, fqdn, created_at, created_by, last_used_at, revoked_at, revoked_by
		 from rotten.api_keys order by id`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Listed, error) {
		var l Listed
		err := r.Scan(&l.ID, &l.Name, &l.FQDN, &l.CreatedAt, &l.CreatedBy, &l.LastUsedAt, &l.RevokedAt, &l.RevokedBy)
		return l, err
	})
}
