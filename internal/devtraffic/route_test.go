package devtraffic_test

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/benchub/rotten/internal/devtraffic"
)

// wantRoutes is the intended routing table, written out independently of
// the catalog: writes and read-your-writes reads on the primary, reporting
// and heavy reads on the replica, the rest split by context.
var wantRoutes = map[string]devtraffic.Route{
	"user_by_id":                 devtraffic.Split,
	"user_by_email":              devtraffic.OnPrimary,
	"touch_user":                 devtraffic.OnPrimary,
	"users_in_list":              devtraffic.Split,
	"search_users":               devtraffic.OnReplica,
	"course_by_id":               devtraffic.Split,
	"courses_in_list":            devtraffic.Split,
	"courses_for_user":           devtraffic.Split,
	"favorites_for_user":         devtraffic.OnPrimary,
	"favorite_courses":           devtraffic.Split,
	"insert_favorite":            devtraffic.OnPrimary,
	"delete_favorite":            devtraffic.OnPrimary,
	"enrollments_for_course":     devtraffic.Split,
	"enrollment_counts":          devtraffic.OnReplica,
	"recompute_final_scores":     devtraffic.OnPrimary,
	"assignments_for_course":     devtraffic.Split,
	"assignment_by_id":           devtraffic.Split,
	"submissions_for_assignment": devtraffic.Split,
	"submission_for_user":        devtraffic.OnPrimary,
	"upsert_submission":          devtraffic.OnPrimary,
	"grade_submission":           devtraffic.OnPrimary,
	"course_grade_summary":       devtraffic.OnReplica,
	"course_activity":            devtraffic.Split,
	"insert_page_view":           devtraffic.OnPrimary,
	"bulk_insert_page_views":     devtraffic.OnPrimary,
	"recent_page_views":          devtraffic.Split,
	"page_view_report":           devtraffic.OnReplica,
	devtraffic.ExportShape:       devtraffic.OnReplica,
	devtraffic.LockShape:         devtraffic.OnPrimary,
	"prune_page_views":           devtraffic.OnPrimary,
}

// wantSplitPercent is, per context, the percentage of its Split shapes'
// runs that go to the replica.
var wantSplitPercent = map[string]int{
	"favorites#list_favorite_courses": 70,
	"favorites#create":                0,
	"favorites#destroy":               0,
	"courses#index":                   50,
	"courses#show":                    30,
	"users#dashboard":                 20,
	"users#search":                    100,
	"login#create":                    0,
	"enrollments_api#index":           90,
	"assignments#show":                40,
	"submissions#create":              0,
	"gradebooks#show":                 80,
	"gradebooks#export":               100,
	"gradebooks#update_submission":    0,

	"Enrollment.recompute_final_score": 0,
	"Submission.auto_grade":            50,
	"PageView.flush_buffer":            0,
	"PageView.prune":                   0,
	"Reports::CourseActivity.generate": 100,
	"Favorite.cleanup_concluded":       0,
	"User.touch_last_seen":             0,
	"Reports::GradeExport.generate":    100,
	"SisImport.process_users":          0,
	"Course.sync_enrollments":          60,
}

// wantPercent is the intended replica percentage for c's runs of s.
func wantPercent(t *testing.T, c devtraffic.Context, s devtraffic.Shape) int {
	t.Helper()
	switch wantRoutes[s.Name] {
	case devtraffic.OnReplica:
		return 100
	case devtraffic.Split:
		p, ok := wantSplitPercent[c.Label()]
		if !ok {
			t.Fatalf("no intended split percentage for context %s", c.Label())
		}
		return p
	}
	return 0
}

func TestRoutingTableIsTheIntendedOne(t *testing.T) {
	for _, s := range devtraffic.Shapes() {
		want, ok := wantRoutes[s.Name]
		if !ok {
			t.Errorf("shape %s has no intended route in this test", s.Name)
			continue
		}
		if got := s.RouteOf(); got != want {
			t.Errorf("shape %s routes %q, want %q", s.Name, got, want)
		}
		for _, c := range s.RunBy() {
			if got, want := devtraffic.ReplicaPercent(c, s), wantPercent(t, c, s); got != want {
				t.Errorf("%s in %s: %d%% to the replica, want %d%%", s.Name, c.Label(), got, want)
			}
		}
	}
	if len(wantRoutes) != len(devtraffic.Shapes()) {
		t.Errorf("the test lists %d shapes, the catalog has %d", len(wantRoutes), len(devtraffic.Shapes()))
	}
	for _, c := range devtraffic.Contexts() {
		if _, ok := wantSplitPercent[c.Label()]; !ok {
			t.Errorf("context %s has no intended split percentage in this test", c.Label())
		}
	}
}

// TestOnlyReadOnlyShapesReachTheReplica checks the hot-standby rule: a shape
// that writes never goes to the replica, whatever its context says.
func TestOnlyReadOnlyShapesReachTheReplica(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for _, s := range devtraffic.Shapes() {
		if s.ReadOnly {
			continue
		}
		if s.RouteOf() != devtraffic.OnPrimary {
			t.Errorf("%s writes but routes %q", s.Name, s.RouteOf())
		}
		for _, c := range devtraffic.Contexts() {
			// Every context, even ones that don't run it, and an
			// all-replica one.
			for _, cc := range []devtraffic.Context{c, {JobTag: "x", SplitReplicaPercent: 100}} {
				if p := devtraffic.ReplicaPercent(cc, s); p != 0 {
					t.Errorf("%s writes but %s sends %d%% to the replica", s.Name, cc.Label(), p)
				}
				for range 200 {
					if devtraffic.PickTarget(cc, s, r) != devtraffic.Primary {
						t.Fatalf("%s writes but PickTarget chose the replica in %s", s.Name, cc.Label())
					}
				}
			}
		}
	}
}

// TestPickTargetFollowsTheRatios draws each (context, shape) pair many times
// and checks the replica share against the intended percentage.
func TestPickTargetFollowsTheRatios(t *testing.T) {
	const n = 20000
	r := rand.New(rand.NewPCG(5, 6))
	for _, c := range devtraffic.Contexts() {
		for _, name := range c.Shapes {
			s, _ := devtraffic.ShapeByName(name)
			want := wantPercent(t, c, s)
			replica := 0
			for range n {
				if devtraffic.PickTarget(c, s, r) == devtraffic.Replica {
					replica++
				}
			}
			got := 100 * float64(replica) / n
			switch want {
			case 0, 100:
				if got != float64(want) {
					t.Errorf("%s in %s: %.2f%% to the replica, want exactly %d%%", name, c.Label(), got, want)
				}
			default:
				// 4 standard deviations of a binomial share at n draws.
				tol := 400 * math.Sqrt(float64(want)/100*(1-float64(want)/100)/n)
				if math.Abs(got-float64(want)) > tol {
					t.Errorf("%s in %s: %.2f%% to the replica, want %d%% ± %.2f", name, c.Label(), got, want, tol)
				}
			}
		}
	}
}

// TestContextReplicaSharesSpread checks the replica utilization reports have
// a spread to show: contexts entirely on the primary, entirely on the
// replica, and several in between on both sides of 50%.
func TestContextReplicaSharesSpread(t *testing.T) {
	var shares []float64
	for _, c := range devtraffic.Contexts() {
		if c.Weight == 0 {
			continue
		}
		share := devtraffic.ContextReplicaShare(c)
		t.Logf("%-34s %5.1f%% of statements on the replica", c.Label(), 100*share)
		shares = append(shares, share)
	}
	var below, above int
	for _, s := range shares {
		switch {
		case s > 0 && s < 0.5:
			below++
		case s >= 0.5 && s < 1:
			above++
		}
	}
	if !slices.Contains(shares, 0) || !slices.Contains(shares, 1) || below < 3 || above < 3 {
		t.Fatalf("replica shares %v, want some at 0%%, some at 100%%, and at least 3 each in (0, 50%%) and [50%%, 100%%)", shares)
	}
}
