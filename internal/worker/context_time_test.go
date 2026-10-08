package worker

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	rottenv1 "github.com/benchub/rotten/gen/rotten/v1"
	"github.com/benchub/rotten/internal/harvestlimits"
)

func aggregateTimes(a *rottenv1.FingerprintAggregate) map[string]*float64 {
	out := map[string]*float64{}
	for _, c := range a.GetContexts() {
		out[contextKey(c.GetController(), c.GetAction(), c.GetJobTag())] = c.Time
	}
	return out
}

// wantServerAccepts runs the server's harvest validation on agg.
func wantServerAccepts(t *testing.T, agg *rottenv1.FingerprintAggregate) {
	t.Helper()
	start := time.Unix(1_700_000_000, 0).UTC()
	end := start.Add(30 * time.Second)
	msg := &rottenv1.SubmitHarvestRequest{
		LogicalSourceId:  1,
		PhysicalSourceId: 2,
		BatchId:          fmt.Sprintf("%d:%d:%d", 2, start.UnixMicro(), end.UnixMicro()),
		WindowStart:      timestamppb.New(start),
		WindowEnd:        timestamppb.New(end),
		Aggregates:       []*rottenv1.FingerprintAggregate{agg},
	}
	if _, _, err := harvestlimits.ValidateHarvest(msg, end, harvestlimits.SkipFutureSkew); err != nil {
		t.Fatalf("server validation rejected the aggregate: %v", err)
	}
}

// The worker ships each context's time, including the untagged context.
// Here they already sum to total_time, so they ship unchanged.
func TestEventAggregateShipsContextTime(t *testing.T) {
	users := contextKey("users", "show", "")
	agg := eventAggregate("fp", "select 1", QueryEvent{
		calls:        10,
		total_time:   100,
		context:      map[string]uint64{users: 6, untaggedContextKey: 4},
		context_time: map[string]float64{users: 95, untaggedContextKey: 5},
	})
	got := aggregateTimes(agg)
	if got[users] == nil || *got[users] != 95 || got[untaggedContextKey] == nil || *got[untaggedContextKey] != 5 {
		t.Fatalf("context times = %v, want users 95 and untagged 5", got)
	}
	wantServerAccepts(t, agg)
}

// pssc's times are execution only; total_time also has planning. Scaling
// up spreads planning across contexts by their execution share, so the
// contexts sum to events.time.
func TestEventAggregateScalesContextTimeUpToTotal(t *testing.T) {
	a, b := contextKey("a", "", ""), contextKey("b", "", "")
	agg := eventAggregate("fp", "select 1", QueryEvent{
		calls:        10,
		total_time:   100,
		context:      map[string]uint64{a: 5, b: 5},
		context_time: map[string]float64{a: 40, b: 40},
	})
	got := aggregateTimes(agg)
	if got[a] == nil || got[b] == nil || *got[a] != 50 || *got[b] != 50 {
		t.Fatalf("context times = %v, want 50 each", got)
	}
	wantServerAccepts(t, agg)
}

// pssc and pgss time separately, so tagged time can exceed pgss's total.
// The worker scales it down so the server doesn't reject the batch.
func TestEventAggregateScalesContextTimeDownToTotal(t *testing.T) {
	a, b := contextKey("a", "", ""), contextKey("b", "", "")
	agg := eventAggregate("fp", "select 1", QueryEvent{
		calls:        10,
		total_time:   0.3,
		context:      map[string]uint64{a: 5, b: 5},
		context_time: map[string]float64{a: 0.3, b: 0.3},
	})
	got := aggregateTimes(agg)
	if got[a] == nil || got[b] == nil || *got[a] != 0.15 || *got[b] != 0.15 {
		t.Fatalf("context times = %v, want 0.15 each", got)
	}
	wantServerAccepts(t, agg)
}

// With no execution time recorded but some total_time (all planning, say),
// the total is split by count.
func TestEventAggregateZeroContextTimeSplitsTotalByCount(t *testing.T) {
	a, b := contextKey("a", "", ""), contextKey("b", "", "")
	agg := eventAggregate("fp", "select 1", QueryEvent{
		calls:        10,
		total_time:   10,
		context:      map[string]uint64{a: 3, b: 7},
		context_time: map[string]float64{a: 0, b: 0},
	})
	got := aggregateTimes(agg)
	if got[a] == nil || got[b] == nil || *got[a] != 3 || *got[b] != 7 {
		t.Fatalf("context times = %v, want 3 and 7", got)
	}
	wantServerAccepts(t, agg)
}

// A subnormal execution sum must not overflow total/sum to Inf.
func TestEventAggregateSubnormalContextTimeStaysFinite(t *testing.T) {
	a, b := contextKey("a", "", ""), contextKey("b", "", "")
	agg := eventAggregate("fp", "select 1", QueryEvent{
		calls:        2,
		total_time:   1e10,
		context:      map[string]uint64{a: 1, b: 1},
		context_time: map[string]float64{a: 5e-324, b: 5e-324},
	})
	got := aggregateTimes(agg)
	if got[a] == nil || got[b] == nil || *got[a] != 5e9 || *got[b] != 5e9 {
		t.Fatalf("context times = %v %v, want 5e9 each", *got[a], *got[b])
	}
	wantServerAccepts(t, agg)
}

// Zero total and zero context time ship zeros.
func TestEventAggregateZeroTotalShipsZeroTimes(t *testing.T) {
	a := contextKey("a", "", "")
	agg := eventAggregate("fp", "select 1", QueryEvent{
		calls:        1,
		context:      map[string]uint64{a: 1},
		context_time: map[string]float64{a: 0},
	})
	if got := aggregateTimes(agg); got[a] == nil || *got[a] != 0 {
		t.Fatalf("context times = %v, want 0", got)
	}
	wantServerAccepts(t, agg)
}

// Float scaling rounds; the server's slack must cover it.
func TestEventAggregateRoundedScalingPassesServerValidation(t *testing.T) {
	a, b, c := contextKey("a", "", ""), contextKey("b", "", ""), contextKey("c", "", "")
	agg := eventAggregate("fp", "select 1", QueryEvent{
		calls:        3,
		total_time:   0.2,
		context:      map[string]uint64{a: 1, b: 1, c: 1},
		context_time: map[string]float64{a: 0.1, b: 0.1, c: 0.1},
	})
	wantServerAccepts(t, agg)

	r := rand.New(rand.NewPCG(1, 2))
	for trial := range 200 {
		n := 1 + r.IntN(500)
		event := QueryEvent{
			calls:        float64(n),
			total_time:   r.Float64() * 1e6,
			context:      map[string]uint64{},
			context_time: map[string]float64{},
		}
		for i := range n {
			k := contextKey(fmt.Sprintf("c%d", i), "", "")
			event.context[k] = 1
			event.context_time[k] = r.Float64() * r.Float64() * 1e4
		}
		agg := eventAggregate("fp", "select 1", event)
		t.Run(fmt.Sprint(trial), func(t *testing.T) { wantServerAccepts(t, agg) })
	}
}

// Without per-context times (none recorded), time stays unset so the server
// uses its proportional estimate rather than storing zeros.
func TestEventAggregateWithoutContextTimeLeavesTimeUnset(t *testing.T) {
	users := contextKey("users", "show", "")
	agg := eventAggregate("fp", "select 1", QueryEvent{
		calls:      10,
		total_time: 100,
		context:    map[string]uint64{users: 10},
	})
	if got := aggregateTimes(agg); got[users] != nil {
		t.Fatalf("time = %v, want unset", *got[users])
	}
}
