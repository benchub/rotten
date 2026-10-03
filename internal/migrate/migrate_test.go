package migrate_test

import (
	"context"
	"testing"

	"github.com/benchub/rotten/internal/migrate"
	"github.com/benchub/rotten/internal/testdb"
)

// schemaSnapshot lists every relation, column, and type in the rotten
// schema, so a second migrate can be shown to change nothing.
const schemaSnapshot = `
select coalesce(string_agg(x, E'\n' order by x), '') from (
	select c.relname || ':' || c.relkind::text || ':' || pg_get_userbyid(c.relowner) as x
	from pg_class c join pg_namespace n on n.oid = c.relnamespace
	where n.nspname = 'rotten'
	union all
	select table_name || '.' || column_name || ':' || data_type
	from information_schema.columns where table_schema = 'rotten'
	union all
	select 'version:' || max(version_id)::text || ':' || count(*)::text from public.goose_db_version
) s`

func TestMigrateUpFromEmptyIsIdempotent(t *testing.T) {
	db := testdb.StartRottenEmpty(t)
	ctx := context.Background()
	dsn := db.DSNAs(t, testdb.OwnerRole)

	applied, err := migrate.Up(ctx, dsn)
	if err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	if len(applied) == 0 || applied[0] != 1 {
		t.Fatalf("first migrate applied %v, want it to start at version 1", applied)
	}

	conn := db.Connect(t)
	var before string
	if err := conn.QueryRow(ctx, schemaSnapshot).Scan(&before); err != nil {
		t.Fatal(err)
	}
	var owner string
	if err := conn.QueryRow(ctx, "select pg_get_userbyid(relowner) from pg_class where oid = 'rotten.events'::regclass").Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if owner != testdb.OwnerRole {
		t.Errorf("rotten.events owner = %q, want %q", owner, testdb.OwnerRole)
	}

	again, err := migrate.Up(ctx, dsn)
	if err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	if len(again) != 0 {
		t.Errorf("second migrate applied %v, want nothing", again)
	}
	var after string
	if err := conn.QueryRow(ctx, schemaSnapshot).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Errorf("second migrate changed the schema:\nbefore:\n%s\nafter:\n%s", before, after)
	}
}

func TestFingerprintStatsLastHoldsBigint(t *testing.T) {
	db := testdb.StartRotten(t)
	conn := db.Connect(t)
	ctx := context.Background()
	const big = int64(1) << 31
	_, err := conn.Exec(ctx, `
		with f as (insert into rotten.fingerprints (fingerprint, normalized) values ('q', 'q') returning id)
		insert into rotten.fingerprint_stats (fingerprint_id, logical_source_id, type, last)
		select id, 0, 'calls', $1 from f`, big)
	if err != nil {
		t.Fatalf("write last = 2^31: %v", err)
	}
	var got int64
	if err := conn.QueryRow(ctx, "select last from rotten.fingerprint_stats").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != big {
		t.Errorf("last = %d, want %d", got, big)
	}
}

func TestFingerprintColumnCommentsDescribeStoredValues(t *testing.T) {
	db := testdb.StartRotten(t)
	conn := db.Connect(t)
	ctx := context.Background()

	want := map[string]string{
		"fingerprint": "Hex string from fingerprint.Normalized, matching proto.rotten.v1.FingerprintAggregate.fingerprint.",
		"normalized":  "pg_query.Normalize output of one representative query text for this fingerprint, stored only on first insert.",
	}
	for column, comment := range want {
		var got string
		if err := conn.QueryRow(ctx, `
			select col_description('rotten.fingerprints'::regclass, a.attnum)
			from pg_attribute a
			where a.attrelid = 'rotten.fingerprints'::regclass
			  and a.attname = $1
			  and not a.attisdropped`, column).Scan(&got); err != nil {
			t.Fatalf("%s comment: %v", column, err)
		}
		if got != comment {
			t.Errorf("%s comment = %q, want %q", column, got, comment)
		}
	}
}

func TestEventContextCountIsBigintEverywhere(t *testing.T) {
	db := testdb.StartRotten(t)
	conn := db.Connect(t)
	ctx := context.Background()

	relations := []string{"rotten.event_context", "rotten.event_context_partition_template"}
	var partition string
	if err := conn.QueryRow(ctx, `
		select (c.oid::regclass)::text
		from pg_inherits i
		join pg_class c on c.oid = i.inhrelid
		where i.inhparent = 'rotten.event_context'::regclass
		order by c.relname
		limit 1`).Scan(&partition); err != nil {
		t.Fatal(err)
	}
	relations = append(relations, partition)

	for _, relation := range relations {
		var dataType string
		if err := conn.QueryRow(ctx, `
			select format_type(a.atttypid, a.atttypmod)
			from pg_attribute a
			where a.attrelid = $1::regclass
			  and a.attname = 'c'
			  and not a.attisdropped`, relation).Scan(&dataType); err != nil {
			t.Fatalf("%s c type: %v", relation, err)
		}
		if dataType != "bigint" {
			t.Errorf("%s.c type = %s, want bigint", relation, dataType)
		}
	}
}
