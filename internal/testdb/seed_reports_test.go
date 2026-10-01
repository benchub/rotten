package testdb

import (
	"context"
	"testing"
)

func TestSeedReports(t *testing.T) {
	db := StartRotten(t)
	r := SeedReports(t, db)
	conn := db.Connect(t)
	ctx := context.Background()

	count := func(q string, args ...any) int {
		t.Helper()
		var n int
		if err := conn.QueryRow(ctx, q, args...).Scan(&n); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return n
	}

	nContexts := 0
	controllers, actions, jobTags := map[string]bool{}, map[string]bool{}, map[string]bool{}
	recent := 0
	for _, e := range ReportEvents {
		nContexts += len(e.Contexts)
		if e.Recent() {
			recent++
		}
		for _, c := range e.Contexts {
			if c.Controller != "" {
				controllers[c.Controller] = true
			}
			if c.Action != "" {
				actions[c.Action] = true
			}
			if c.JobTag != "" {
				jobTags[c.JobTag] = true
			}
		}
	}
	if recent == 0 || recent == len(ReportEvents) {
		t.Fatalf("spec needs both recent and old events, has %d recent of %d", recent, len(ReportEvents))
	}

	for _, c := range []struct {
		q    string
		want int
	}{
		{"select count(*) from rotten.logical_sources where id <> 0", len(ReportSources)},
		{"select count(distinct project) from rotten.logical_sources where id <> 0", len(ReportProjects())},
		{"select count(*) from rotten.physical_sources", len(ReportSources)},
		{"select count(*) from rotten.fingerprints", len(ReportFingerprints)},
		{"select count(*) from rotten.events", len(ReportEvents)},
		{"select count(*) from rotten.event_context", nContexts},
		{"select count(*) from rotten.controllers", len(controllers)},
		{"select count(*) from rotten.actions", len(actions)},
		{"select count(*) from rotten.job_tags", len(jobTags)},
		{"select count(*) from rotten.fingerprint_stats", len(ReportStats)},
		{"select count(*) from rotten.events where observed_window_start >= now() - interval '3 hours' and observed_window_end <= now()", recent},
	} {
		if got := count(c.q); got != c.want {
			t.Errorf("%s = %d, want %d", c.q, got, c.want)
		}
	}

	for key, want := range r.TotalsBySource {
		var calls, tm float64
		if err := conn.QueryRow(ctx, "select sum(calls), sum(time) from rotten.events where logical_source_id = $1",
			r.SourceIDs[key]).Scan(&calls, &tm); err != nil {
			t.Fatal(err)
		}
		if calls != want.Calls || tm != want.Time {
			t.Errorf("source %s: calls %v time %v, want %v %v", key, calls, tm, want.Calls, want.Time)
		}
	}
	if len(r.TotalsBySource) != len(ReportSources) {
		t.Errorf("totals for %d sources, want %d", len(r.TotalsBySource), len(ReportSources))
	}
	var rc float64
	if err := conn.QueryRow(ctx, `select coalesce(sum(calls),0) from rotten.events
		where logical_source_id = $1 and observed_window_start >= $2::timestamptz - interval '3 hours' and observed_window_end <= $2`,
		r.SourceIDs["canvas13p"], r.Anchor).Scan(&rc); err != nil {
		t.Fatal(err)
	}
	if want := r.RecentTotalsBySource["canvas13p"].Calls; rc != want || want == 0 {
		t.Errorf("canvas13p recent calls %v, want %v (nonzero)", rc, want)
	}

	// RecentTotals matches the database, group by group.
	groups := RecentTotals()
	rows, err := conn.Query(ctx, `select s.project, s.cluster, f.fingerprint, sum(calls), sum(time)
		from rotten.events e join rotten.logical_sources s on s.id = e.logical_source_id
		join rotten.fingerprints f on f.id = e.fingerprint_id
		where observed_window_start >= $1::timestamptz - interval '3 hours' and observed_window_end <= $1
		group by 1, 2, 3`, r.Anchor)
	if err != nil {
		t.Fatal(err)
	}
	fpKey := map[string]string{}
	for _, f := range ReportFingerprints {
		fpKey[f.Fingerprint] = f.Key
	}
	nGroups := 0
	for rows.Next() {
		var k GroupKey
		var tot SeedTotals
		if err := rows.Scan(&k.Project, &k.Cluster, &k.Fingerprint, &tot.Calls, &tot.Time); err != nil {
			t.Fatal(err)
		}
		k.Fingerprint = fpKey[k.Fingerprint]
		nGroups++
		if groups[k] != tot {
			t.Errorf("group %+v: db %+v, spec %+v", k, tot, groups[k])
		}
	}
	rows.Close()
	if nGroups != len(groups) {
		t.Errorf("db has %d recent groups, spec %d", nGroups, len(groups))
	}

	// The top-five helper cuts canvas 13 users down from seven contexts.
	top := RecentTopContexts(GroupKey{"canvas", "13", "users"}, 5)
	if len(top) != 5 || top[0] != (SeedContext{"users", "show", "", 600}) || top[1] != (SeedContext{"grades", "show", "", 200}) {
		t.Errorf("top contexts = %+v", top)
	}

	// Every row lands in its own daily partition, not the default one.
	for _, parent := range []string{"events", "event_context"} {
		rows, err := conn.Query(ctx, `select tableoid::regclass::text, observed_window_start::text from rotten.`+parent)
		if err != nil {
			t.Fatal(err)
		}
		type row struct{ part, start string }
		var got []row
		for rows.Next() {
			var x row
			if err := rows.Scan(&x.part, &x.start); err != nil {
				t.Fatal(err)
			}
			got = append(got, x)
		}
		rows.Close()
		for _, x := range got {
			var want string
			if err := conn.QueryRow(ctx, "select (partition_schema||'.'||partition_table)::regclass::text from public.show_partition_name($1, $2)",
				"rotten."+parent, x.start).Scan(&want); err != nil {
				t.Fatal(err)
			}
			if x.part != want {
				t.Errorf("%s row at %s is in %s, want %s", parent, x.start, x.part, want)
			}
		}
	}
}
