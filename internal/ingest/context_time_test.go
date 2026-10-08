package ingest_test

import (
	"context"
	"math"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
)

// contextTimes maps "controller#action#job_tag" (empty for NULL) to the
// stored attributed_time.
func contextTimes(t *testing.T, f *submitFixture) map[string]float64 {
	t.Helper()
	rows, err := f.owner.Query(context.Background(), `
		select coalesce(c.controller, '') || '#' || coalesce(a.action, '') || '#' || coalesce(j.job_tag, ''), ec.attributed_time
		from rotten.event_context ec
		left join rotten.controllers c on c.id = ec.controller_id
		left join rotten.actions a on a.id = ec.action_id
		left join rotten.job_tags j on j.id = ec.job_tag_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]float64{}
	for rows.Next() {
		var key string
		var v float64
		if err := rows.Scan(&key, &v); err != nil {
			t.Fatal(err)
		}
		out[key] = v
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func timedHarvest(f *submitFixture, suffix string, contexts ...*rottenv1.QueryContext) *connect.Request[rottenv1.SubmitHarvestRequest] {
	start := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)
	req := harvestRequest(f.reg.GetLogicalSourceId(), f.reg.GetPhysicalSourceId(), start, suffix, "fp-"+suffix)
	agg := req.Msg.Aggregates[0]
	agg.Metrics.Calls = 10
	agg.Metrics.TotalTime = 100
	agg.Contexts = contexts
	return req
}

// Contexts that carry their own time are stored with it, not split by count.
func TestSubmitHarvestStoresShippedContextTime(t *testing.T) {
	f := setupSubmit(t)
	req := timedHarvest(f, "ctx-time",
		&rottenv1.QueryContext{Controller: "users", Action: "show", Count: 5, Time: proto.Float64(90)},
		&rottenv1.QueryContext{Controller: "users", Action: "index", Count: 5, Time: proto.Float64(10)},
	)
	if _, err := f.handler.SubmitHarvest(f.ctx, req); err != nil {
		t.Fatalf("SubmitHarvest: %v", err)
	}
	got := contextTimes(t, f)
	if got["users#show#"] != 90 || got["users#index#"] != 10 || len(got) != 2 {
		t.Fatalf("attributed times = %v, want users#show 90 and users#index 10", got)
	}
}

// Older workers don't send time, so the server keeps the proportional estimate.
func TestSubmitHarvestWithoutContextTimeKeepsProportionalEstimate(t *testing.T) {
	f := setupSubmit(t)
	req := timedHarvest(f, "ctx-old",
		&rottenv1.QueryContext{Controller: "users", Action: "show", Count: 6},
		&rottenv1.QueryContext{Controller: "users", Action: "index", Count: 4},
	)
	if _, err := f.handler.SubmitHarvest(f.ctx, req); err != nil {
		t.Fatalf("SubmitHarvest: %v", err)
	}
	got := contextTimes(t, f)
	if got["users#show#"] != 60 || got["users#index#"] != 40 {
		t.Fatalf("attributed times = %v, want 60 and 40", got)
	}
}

// An explicit zero is a real time, not "absent".
func TestSubmitHarvestStoresZeroContextTime(t *testing.T) {
	f := setupSubmit(t)
	req := timedHarvest(f, "ctx-zero",
		&rottenv1.QueryContext{Controller: "users", Action: "show", Count: 5, Time: proto.Float64(0)},
		&rottenv1.QueryContext{Controller: "users", Action: "index", Count: 5, Time: proto.Float64(100)},
	)
	if _, err := f.handler.SubmitHarvest(f.ctx, req); err != nil {
		t.Fatalf("SubmitHarvest: %v", err)
	}
	got := contextTimes(t, f)
	if got["users#show#"] != 0 || got["users#index#"] != 100 {
		t.Fatalf("attributed times = %v, want 0 and 100", got)
	}
}

// The untagged context (all three tags empty) is stored with all IDs NULL,
// with its time, and fingerprint_contexts.sql returns it.
func TestSubmitHarvestStoresUntaggedContextForReports(t *testing.T) {
	f := setupSubmit(t)
	req := timedHarvest(f, "ctx-untagged",
		&rottenv1.QueryContext{Count: 7, Time: proto.Float64(30)},
		&rottenv1.QueryContext{Controller: "users", Action: "show", Count: 3, Time: proto.Float64(70)},
	)
	start := req.Msg.GetWindowStart().AsTime()
	if _, err := f.handler.SubmitHarvest(f.ctx, req); err != nil {
		t.Fatalf("SubmitHarvest: %v", err)
	}
	if got := contextTimes(t, f); got["##"] != 30 {
		t.Fatalf("attributed times = %v, want untagged 30", got)
	}

	var fpID int64
	if err := f.owner.QueryRow(context.Background(), `select id from rotten.fingerprints where fingerprint = 'fp-ctx-untagged'`).Scan(&fpID); err != nil {
		t.Fatal(err)
	}
	query, err := os.ReadFile("../../reports/fingerprint_contexts.sql")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := f.owner.Query(context.Background(), string(query),
		"submit", "test", "cluster", fpID, start.Add(-time.Second), start.Add(time.Minute), 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	foundUntagged := false
	n := 0
	for rows.Next() {
		var controller, action, jobTag *string
		var times float64
		if err := rows.Scan(&controller, &action, &jobTag, &times); err != nil {
			t.Fatal(err)
		}
		n++
		if controller == nil && action == nil && jobTag == nil && times == 7 {
			foundUntagged = true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !foundUntagged || n != 2 {
		t.Fatalf("fingerprint_contexts returned %d rows, untagged found = %v; want 2 rows with untagged times 7", n, foundUntagged)
	}
}

func TestSubmitHarvestValidationRejectsBadContextTime(t *testing.T) {
	cases := map[string][]*rottenv1.QueryContext{
		"negative": {{Controller: "a", Count: 5, Time: proto.Float64(-1)}},
		"nan":      {{Controller: "a", Count: 5, Time: proto.Float64(math.NaN())}},
		"inf":      {{Controller: "a", Count: 5, Time: proto.Float64(math.Inf(1))}},
		"sum exceeds total_time": {
			{Controller: "a", Count: 5, Time: proto.Float64(60)},
			{Controller: "b", Count: 5, Time: proto.Float64(40.01)},
		},
		"mixed set and unset": {
			{Controller: "a", Count: 5, Time: proto.Float64(10)},
			{Controller: "b", Count: 5},
		},
	}
	for name, contexts := range cases {
		t.Run(name, func(t *testing.T) {
			f := setupSubmit(t)
			req := timedHarvest(f, "bad-time", contexts...)
			_, err := f.handler.SubmitHarvest(f.ctx, req)
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Fatalf("SubmitHarvest err = %v, want InvalidArgument", err)
			}
			wantNoSubmitWrites(t, f.owner)
		})
	}
}

// Float summation slack: a sum a hair over total_time is accepted.
func TestSubmitHarvestAcceptsContextTimeWithinTolerance(t *testing.T) {
	f := setupSubmit(t)
	req := timedHarvest(f, "ctx-tol",
		&rottenv1.QueryContext{Controller: "a", Count: 5, Time: proto.Float64(60)},
		&rottenv1.QueryContext{Controller: "b", Count: 5, Time: proto.Float64(40 + 1e-8)},
	)
	if _, err := f.handler.SubmitHarvest(f.ctx, req); err != nil {
		t.Fatalf("SubmitHarvest: %v", err)
	}
}
