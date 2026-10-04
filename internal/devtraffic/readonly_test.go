package devtraffic_test

import (
	"context"
	"math/rand/v2"
	"net/url"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/benchub/rotten/internal/devtraffic"
	"github.com/benchub/rotten/internal/testdb"
)

// TestShapeReadOnlyFlags checks each shape's ReadOnly flag the way a hot
// standby would: a read-only shape runs in a READ ONLY transaction, and any
// other shape is refused there.
func TestShapeReadOnlyFlags(t *testing.T) {
	db := testdb.StartObserved(t, 18)
	ctx := context.Background()
	sz, err := devtraffic.Setup(ctx, db.DSN, 1, 0.05)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(db.DSN)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = devtraffic.ShardSchema(1)
	conn, err := pgx.Connect(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	r := rand.New(rand.NewPCG(3, 4))
	m := devtraffic.Meta{ContextID: "1", Hostname: "h", PID: 1}
	reads := 0
	for _, s := range devtraffic.Shapes() {
		if s.ReadOnly {
			reads++
		}
		by := s.RunBy()
		if len(by) == 0 {
			t.Errorf("%s: no contexts run it", s.Name)
			continue
		}
		sql, args := s.Render(by[0], m, devtraffic.ShardSchema(1), devtraffic.Trailing, r, sz)
		tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, sql, args...)
		_ = tx.Rollback(ctx)
		switch {
		case s.ReadOnly && err != nil:
			t.Errorf("%s is flagged read-only but fails in a read-only transaction: %v", s.Name, err)
		case !s.ReadOnly && err == nil:
			t.Errorf("%s is not flagged read-only but runs in a read-only transaction", s.Name)
		}
	}
	if reads < 10 || reads == len(devtraffic.Shapes()) {
		t.Errorf("%d of %d shapes read-only; want a real mix", reads, len(devtraffic.Shapes()))
	}
}
