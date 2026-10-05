package devtraffic

import (
	"fmt"
	"math/rand/v2"
)

// Route is where a shape runs when the generator has a replica. Without one,
// everything runs on the primary.
type Route string

const (
	// OnPrimary runs the shape on the primary only: every write, and reads
	// that must see the request's own writes. A Shape's zero Route means
	// OnPrimary.
	OnPrimary Route = "primary"
	// OnReplica runs the shape on the replica only: reporting and heavy
	// reads.
	OnReplica Route = "replica"
	// Split sends each run to the replica with the running context's
	// SplitReplicaPercent, and otherwise to the primary.
	Split Route = "split"
)

// Target is the server one statement runs on.
type Target int

const (
	Primary Target = iota
	Replica
)

func (t Target) String() string {
	if t == Replica {
		return "replica"
	}
	return "primary"
}

// RouteOf is s's route, OnPrimary if unset.
func (s Shape) RouteOf() Route {
	if s.Route == "" {
		return OnPrimary
	}
	return s.Route
}

// ReplicaPercent is the percentage of c's runs of s that go to the replica:
// 0 for OnPrimary and for any shape that isn't ReadOnly, 100 for OnReplica,
// and c's SplitReplicaPercent for Split.
func ReplicaPercent(c Context, s Shape) int {
	if !s.ReadOnly {
		return 0
	}
	switch s.RouteOf() {
	case OnReplica:
		return 100
	case Split:
		return min(100, max(0, c.SplitReplicaPercent))
	}
	return 0
}

// PickTarget chooses where one run of s in context c goes.
func PickTarget(c Context, s Shape, r *rand.Rand) Target {
	switch p := ReplicaPercent(c, s); {
	case p <= 0:
		return Primary
	case p >= 100:
		return Replica
	case r.IntN(100) < p:
		return Replica
	}
	return Primary
}

// ContextReplicaShare is the expected fraction of c's statements that run
// on the replica, which is what the replica utilization reports show for c.
func ContextReplicaShare(c Context) float64 {
	if len(c.Shapes) == 0 {
		return 0
	}
	total := 0
	for _, name := range c.Shapes {
		s, _ := ShapeByName(name)
		total += ReplicaPercent(c, s)
	}
	return float64(total) / float64(100*len(c.Shapes))
}

// checkRoutes rejects a routing table that would send a write to a hot
// standby, names an unknown route, or has a percentage outside 0 to 100.
func checkRoutes(shapes []Shape, contexts []Context) error {
	for _, s := range shapes {
		switch s.RouteOf() {
		case OnPrimary:
		case OnReplica, Split:
			if !s.ReadOnly {
				return fmt.Errorf("devtraffic: shape %s writes, so it can't route %q", s.Name, s.Route)
			}
		default:
			return fmt.Errorf("devtraffic: shape %s has unknown route %q", s.Name, s.Route)
		}
	}
	for _, c := range contexts {
		if c.SplitReplicaPercent < 0 || c.SplitReplicaPercent > 100 {
			return fmt.Errorf("devtraffic: context %s SplitReplicaPercent %d, want 0 to 100", c.Label(), c.SplitReplicaPercent)
		}
	}
	return nil
}

func init() {
	if err := checkRoutes(shapes, contexts); err != nil {
		panic(err)
	}
}
