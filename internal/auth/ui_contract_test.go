package auth_test

// The contract between the UI's pass key pages (ui/) and the server's auth.
// The UI is Ruby, so these tests pin what the two sides share: the secret,
// hash and token format, through test vectors that
// ui/spec/lib/rotten_ui/pass_key_spec.rb checks too, and the SQL the UI runs,
// read from ui/app/sql and run here as rotten_ui.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/benchub/rotten/internal/auth"
	"github.com/benchub/rotten/internal/testdb"
)

type passKeyVector struct {
	ID     int64  `json:"id"`
	Secret string `json:"secret"`
	Hash   string `json:"hash"`
	Token  string `json:"token"`
}

func uiFile(t *testing.T, parts ...string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(append([]string{"..", "..", "ui"}, parts...)...))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func passKeyVectors(t *testing.T) []passKeyVector {
	t.Helper()
	var doc struct {
		Vectors []passKeyVector `json:"vectors"`
	}
	if err := json.Unmarshal(uiFile(t, "spec", "fixtures", "pass_key_vectors.json"), &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Vectors) < 2 {
		t.Fatalf("got %d pass key vectors, want at least 2", len(doc.Vectors))
	}
	return doc.Vectors
}

func TestPassKeyVectorsSharedWithUI(t *testing.T) {
	for _, v := range passKeyVectors(t) {
		if got := auth.HashSecret(v.Secret); got != v.Hash {
			t.Errorf("HashSecret(%q) = %s, want %s", v.Secret, got, v.Hash)
		}
		if got := auth.FormatKey(v.ID, v.Secret); got != v.Token {
			t.Errorf("FormatKey(%d) = %s, want %s", v.ID, got, v.Token)
		}
		id, secret, err := auth.ParseKey(v.Token)
		if err != nil || id != v.ID || secret != v.Secret {
			t.Errorf("ParseKey(%s) = %d, %q, %v; want %d, %q", v.Token, id, secret, err, v.ID, v.Secret)
		}
	}
}

// A key created and revoked with the UI's SQL, as rotten_ui, works until
// it's revoked and then stops working within the cache TTL.
func TestUIRevokedKeyRejectedWithinTTL(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	ui, err := pgxpool.New(ctx, f.db.DSNAs(t, testdb.UIRole))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ui.Close)
	createSQL := string(uiFile(t, "app", "sql", "api_keys", "create.sql"))
	revokeSQL := string(uiFile(t, "app", "sql", "api_keys", "revoke.sql"))
	auditSQL := string(uiFile(t, "app", "sql", "ui_audit_log", "insert.sql"))

	v := passKeyVectors(t)[0]
	var id int64
	if err := ui.QueryRow(ctx, createSQL, "ui-w1", v.Hash, "db1.example.com", "admin@example.com").Scan(&id); err != nil {
		t.Fatalf("create as rotten_ui: %v", err)
	}
	if _, err := ui.Exec(ctx, auditSQL, 7, "admin@example.com", "api_key.create", "api_key", id, `{"name":"ui-w1"}`); err != nil {
		t.Fatalf("audit as rotten_ui: %v", err)
	}
	token := "Bearer " + auth.FormatKey(id, v.Secret)
	if err := f.call(token); err != nil {
		t.Fatalf("UI-created key: %v", err)
	}
	if got := f.stub.got[0]; got.ID != id || got.Name != "ui-w1" || got.FQDN != "db1.example.com" {
		t.Errorf("authenticated as %+v, want id %d ui-w1 pinned to db1.example.com", got, id)
	}

	var revoked int64
	if err := ui.QueryRow(ctx, revokeSQL, id, "admin@example.com").Scan(&revoked); err != nil || revoked != id {
		t.Fatalf("revoke as rotten_ui: id %d, %v", revoked, err)
	}
	if err := ui.QueryRow(ctx, revokeSQL, id, "other@example.com").Scan(&revoked); err != pgx.ErrNoRows {
		t.Fatalf("second revoke: %v, want no rows", err)
	}
	var by string
	if err := f.owner.QueryRow(ctx, "select revoked_by from rotten.api_keys where id = $1", id).Scan(&by); err != nil || by != "admin@example.com" {
		t.Fatalf("revoked_by = %q, %v; want the first revoker", by, err)
	}

	f.clk.Add(10 * time.Second)
	if err := f.call(token); err != nil {
		t.Fatalf("inside TTL, cached: %v", err)
	}
	f.clk.Add(21 * time.Second)
	wantCode(t, f.call(token), connect.CodeUnauthenticated, "revoked by the UI, after TTL")
}
