package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/pssc"
	"github.com/benchub/rotten/internal/testdb"
	"github.com/jackc/pgx/v5"
)

// reloadPSSC sets one pssc setting with ALTER SYSTEM and waits until this
// connection sees it.
func reloadPSSC(t *testing.T, su *pgx.Conn, name, v string) {
	t.Helper()
	ctx := context.Background()
	for _, q := range []string{
		"alter system set pg_stat_statement_context." + name + " = '" + v + "'",
		"select pg_reload_conf()",
	} {
		if _, err := su.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	var last string
	for range 50 {
		if err := su.QueryRow(ctx, "show pg_stat_statement_context."+name).Scan(&last); err != nil {
			t.Fatal(err)
		}
		if last == v {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s never became %q (last %q)", name, v, last)
}

// Dropping pssc while connected logs "not in use" once, at the next harvest.
func TestPSSCDroppedWhileConnectedLogsNotInUse(t *testing.T) {
	db := startObservedForWorker(t)
	su := db.Connect(t)
	var logs syncBuffer
	rn := startLoggingWorker(t, db, &logs)
	if _, err := su.Exec(context.Background(), "drop extension pg_stat_statement_context"); err != nil {
		t.Fatal(err)
	}
	rn.window(t)
	rn.window(t)
	got := logs.String()
	if n := strings.Count(got, psscNotInUseMessage); n != 1 || !strings.Contains(got, "the extension isn't created") {
		t.Fatalf("not-in-use logged %d times, want 1 with the reason: %s", n, got)
	}
}

// A reload that makes extractors append-only warns at the next harvest, once.
func TestPSSCExtractorsChangedByReloadWarns(t *testing.T) {
	db := startObservedVersionForWorker(t, 16, testdb.PSSCExtractorsFromConfig())
	su := db.Connect(t)
	reloadPSSC(t, su, "extractors", testdb.PSSCExtractors)
	var logs syncBuffer
	rn := startLoggingWorker(t, db, &logs)
	if strings.Contains(logs.String(), psscExtractorsWarning) {
		t.Fatalf("warned before the change: %s", logs.String())
	}
	reloadPSSC(t, su, "extractors", "sqlcommenter, marginalia")
	rn.window(t)
	rn.window(t)
	if n := strings.Count(logs.String(), psscExtractorsWarning); n != 1 {
		t.Fatalf("extractors warned %d times, want 1: %s", n, logs.String())
	}
}

// An unchanged bad value warns on the first connection only, not again on
// a reconnect.
func TestPSSCBadSettingQuietAcrossReconnect(t *testing.T) {
	db := startObservedVersionForWorker(t, 16, testdb.PSSCTagsFromConfig())
	reloadPSSC(t, db.Connect(t), "tags", "action, job")
	var logs syncBuffer
	w := loggingWorker(&logs)
	for range 2 {
		w.checkPSSCAtConnect(context.Background(), pssc.NewReader(db.ConnectAs(t, "rotten_observer")))
	}
	if n := strings.Count(logs.String(), psscTagsWarning); n != 1 {
		t.Fatalf("tags warned %d times over two connections, want 1: %s", n, logs.String())
	}
	if n := strings.Count(logs.String(), psscInUseMessage); n != 2 {
		t.Fatalf("in-use logged %d times, want once per connection: %s", n, logs.String())
	}
}

// When the settings read fails, the harvest still runs the
// utility_missing_queryid check, and the error warns once, not every
// harvest.
func TestPSSCSettingsErrorWarnsOnceAndKeepsUtilityCheck(t *testing.T) {
	db := startObservedForWorker(t)
	conn := db.Connect(t)
	ctx := context.Background()
	var logs syncBuffer
	w := loggingWorker(&logs)
	r := pssc.NewReader(conn)
	w.checkPSSCAtConnect(ctx, r)
	// Shadow pg_settings with a view that fails, so only the settings read
	// breaks.
	for _, q := range []string{
		"create schema shadow",
		"create function shadow.boom() returns bool language plpgsql as $$begin raise exception 'settings unavailable'; end$$",
		"create view shadow.pg_settings as select name, setting from pg_catalog.pg_settings where shadow.boom()",
		"set search_path = shadow, pg_catalog, public",
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for range 3 {
		_ = w.checkPSSCHarvest(ctx, r, true)
	}
	if !w.psscWatch.haveLast {
		t.Fatal("utility_missing_queryid wasn't read after the settings read failed")
	}
	if n := strings.Count(logs.String(), "settings unavailable"); n != 1 || !strings.Contains(logs.String(), "level=WARN") {
		t.Fatalf("settings error logged %d times, want 1 warning: %s", n, logs.String())
	}
}
