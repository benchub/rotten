package reports_test

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"regexp"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/benchub/rotten/internal/testdb"
	"github.com/jackc/pgx/v5"
)

// TestOutliersMatchesHistoryDefinition checks outliers.sql against a Go
// reading of its header (History, Baseline and Outlier definition), on
// generated data: frequent and rare fingerprints, a replica whose two hosts
// often share a window (so the lookback's last window can hold two samples),
// windows exactly on the lookback's and the 7 days' bounds, windows
// straddling the range's start, calls = 0 rows, a second cluster that must
// never count, and contexts and query texts for the match filter. With a
// very low threshold every scored group is listed, so each one's history
// sample count, median and spread is compared, not just the top few.
func TestOutliersMatchesHistoryDefinition(t *testing.T) {
	db := testdb.StartRotten(t)
	conn := db.Connect(t)
	d := seedOutlierOracle(t, conn)

	h3 := d.anchor.Add(-3 * time.Hour)
	all := -1e12
	for _, c := range []oracleCase{
		{name: "3h", start: h3, end: d.anchor, sigma: all, limit: 1_000_000},
		{name: "3h top 10", start: h3, end: d.anchor, sigma: defaultSigma, limit: 10},
		{name: "3h primary", start: h3, end: d.anchor, role: testdb.ReportPrimaryRole, sigma: all, limit: 1_000_000},
		{name: "3h replica", start: h3, end: d.anchor, role: testdb.ReportReplicaRole, sigma: all, limit: 1_000_000},
		{name: "3h controller match", start: h3, end: d.anchor, match: "^alpha#", sigma: all, limit: 1_000_000},
		{name: "3h job match", start: h3, end: d.anchor, match: "nightly", sigma: all, limit: 1_000_000},
		{name: "3h text match", start: h3, end: d.anchor, match: "oracle_1[0-9] ", sigma: all, limit: 1_000_000},
		{name: "3h no match", start: h3, end: d.anchor, match: "nothing matches this", sigma: all, limit: 1_000_000},
		{name: "30m", start: d.anchor.Add(-30 * time.Minute), end: d.anchor, sigma: all, limit: 1_000_000},
		{name: "24h", start: d.anchor.Add(-24 * time.Hour), end: d.anchor, sigma: all, limit: 1_000_000},
		{name: "2d", start: d.anchor.Add(-48 * time.Hour), end: d.anchor, sigma: all, limit: 1_000_000},
		{name: "7d", start: d.anchor.Add(-7 * 24 * time.Hour), end: d.anchor, sigma: all, limit: 1_000_000},
		{name: "3h two days ago", start: h3.Add(-48 * time.Hour), end: d.anchor.Add(-48 * time.Hour), sigma: all, limit: 1_000_000},
	} {
		t.Run(c.name, func(t *testing.T) {
			want := d.outliers(c)
			var role, match any
			if c.role != "" {
				role = c.role
			}
			if c.match != "" {
				match = c.match
			}
			got := readOutliers(t, conn, oracleProject, testdb.ReportEnvironment, oracleCluster, c.start, c.end,
				c.limit, c.sigma, defaultMinHistory, defaultRatio, role, match)
			if len(want) < 3 && c.match != "nothing matches this" {
				t.Fatalf("the oracle scores only %d groups; the data doesn't exercise the report", len(want))
			}
			if len(got) != len(want) {
				t.Errorf("%d rows, want %d", len(got), len(want))
			}
			for i := range min(len(got), len(want)) {
				g, w := got[i], want[i]
				if g.LogicalSourceID != w.source || g.FingerprintID != w.fingerprint {
					t.Errorf("row %d is (source %d, fingerprint %d), want (%d, %d)", i, g.LogicalSourceID, g.FingerprintID, w.source, w.fingerprint)
					continue
				}
				if g.HistorySamples != int64(w.samples) || !closeTo(g.HistoryMedianMS, w.median) ||
					!closeTo(g.HistorySpreadMS, w.spread) || !closeTo(g.Score, w.score) {
					t.Errorf("(source %d, fingerprint %d): samples %d median %v spread %v score %v, want %d %v %v %v",
						w.source, w.fingerprint, g.HistorySamples, g.HistoryMedianMS, g.HistorySpreadMS, g.Score,
						w.samples, w.median, w.spread, w.score)
				}
			}
			if c.name == "3h" {
				var counts []int
				for _, w := range want {
					if w.fingerprint == d.bound {
						counts = append(counts, w.samples)
					}
				}
				slices.Sort(counts)
				if !slices.Equal(counts, []int{30, 31}) {
					t.Errorf("the oracle gives the 7-day-bound fingerprint %v history samples, want [30 31]", counts)
				}
			}
			extended := 0
			for _, w := range want {
				if w.extended {
					extended++
				}
			}
			t.Logf("%d rows, %d of them with history from before the default lookback", len(want), extended)
		})
	}
}

const (
	oracleProject = "oracle"
	oracleCluster = "1"
)

type oracleCase struct {
	name       string
	start, end time.Time
	role       string
	match      string
	sigma      float64
	limit      int
}

type oracleEvent struct {
	source      int
	fingerprint int64
	start, end  time.Time
	calls, time float64
	contexts    []string // "controller#action", or the job tag
}

type oracleData struct {
	anchor     time.Time
	roles      map[int]string // logical source id -> role, for cluster 1
	normalized map[int64]string
	events     []oracleEvent
	bound      int64
}

type oracleRow struct {
	source       int
	fingerprint  int64
	worst, total float64
	samples      int
	median       float64
	spread       float64
	score        float64
	extended     bool
}

func closeTo(a, b float64) bool {
	return a == b || math.Abs(a-b) <= 1e-9*math.Max(math.Abs(a), math.Abs(b))
}

// seedOutlierOracle writes 9 days of events, then reads them back with
// their contexts, so the oracle sees exactly what the report sees.
func seedOutlierOracle(t *testing.T, conn *pgx.Conn) *oracleData {
	t.Helper()
	ctx := context.Background()
	d := &oracleData{roles: map[int]string{}, normalized: map[int64]string{}}
	if err := conn.QueryRow(ctx, "select date_trunc('minute', now())").Scan(&d.anchor); err != nil {
		t.Fatal(err)
	}

	type host struct{ logical, physical int }
	var primary, replica1, replica2, elsewhere host
	for _, s := range []struct {
		h                *host
		cluster, role    string
		fqdn             string
		reuseLogicalFrom *host
	}{
		{&primary, oracleCluster, testdb.ReportPrimaryRole, "oracle-p1", nil},
		{&replica1, oracleCluster, testdb.ReportReplicaRole, "oracle-r1", nil},
		{&replica2, oracleCluster, testdb.ReportReplicaRole, "oracle-r2", &replica1},
		{&elsewhere, "2", testdb.ReportPrimaryRole, "oracle-other", nil},
	} {
		if s.reuseLogicalFrom != nil {
			s.h.logical = s.reuseLogicalFrom.logical
		} else if err := conn.QueryRow(ctx, `insert into rotten.logical_sources (project, environment, cluster, role)
			values ($1, $2, $3, $4) returning id`, oracleProject, testdb.ReportEnvironment, s.cluster, s.role).Scan(&s.h.logical); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRow(ctx, `insert into rotten.physical_sources (fqdn) values ($1) returning id`, s.fqdn).Scan(&s.h.physical); err != nil {
			t.Fatal(err)
		}
		if s.cluster == oracleCluster {
			d.roles[s.h.logical] = s.role
		}
	}

	const fingerprints = 40
	var ids []int64
	for i := range fingerprints {
		normalized := fmt.Sprintf("select oracle_%d from t%d", i, i%4)
		var id int64
		if err := conn.QueryRow(ctx, `insert into rotten.fingerprints (fingerprint, normalized) values ($1, $2) returning id`,
			fmt.Sprintf("oracle-%d", i), normalized).Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
		d.normalized[id] = normalized
	}

	rng := rand.New(rand.NewPCG(20261004, 231500))
	h3 := d.anchor.Add(-3 * time.Hour)
	first := d.anchor.Add(-9 * 24 * time.Hour)
	var rows [][]any
	add := func(h host, fp int64, start time.Time, length time.Duration, base float64) {
		calls := float64(1 + rng.IntN(50))
		if rng.IntN(40) == 0 {
			calls = 0
		}
		// Rounded to a tenth so samples, and so medians, sometimes tie.
		ms := math.Round(base*(0.6+0.8*rng.Float64())*10) / 10
		rows = append(rows, []any{fp, h.logical, h.physical, start, start.Add(length), calls, ms * calls})
	}
	periods := []time.Duration{5 * time.Minute, 20 * time.Minute, time.Hour, 2 * time.Hour, 3 * time.Hour, 5 * time.Hour, 8 * time.Hour, 13 * time.Hour}
	for i, fp := range ids {
		base := 1 + float64(i%9)
		period := periods[i%len(periods)]
		offset := time.Duration(rng.IntN(int(period/time.Minute))) * time.Minute
		for start := first.Add(offset); start.Before(d.anchor.Add(-5 * time.Minute)); start = start.Add(period) {
			// A slow spell in the last hour for some, so the default
			// threshold lists a few.
			b := base
			if i%5 == 0 && !start.Before(d.anchor.Add(-time.Hour)) {
				b *= 6
			}
			if rng.IntN(10) > 0 {
				add(primary, fp, start, 5*time.Minute, b)
			}
			switch rng.IntN(4) {
			case 0, 1: // both replica hosts in the same window: tied samples
				add(replica1, fp, start, 5*time.Minute, b)
				add(replica2, fp, start, 5*time.Minute, b)
			case 2:
				add(replica1, fp, start, 5*time.Minute, b)
				add(replica2, fp, start.Add(time.Minute), 5*time.Minute, b)
			case 3:
				add(replica2, fp, start, 5*time.Minute, b)
			}
			if rng.IntN(3) == 0 {
				add(elsewhere, fp, start, 5*time.Minute, b*10)
			}
		}
		// Every group has a window in the last 3 hours, some slow.
		in := h3.Add(time.Duration(10+rng.IntN(160)) * time.Minute)
		add(primary, fp, in, 5*time.Minute, base*float64(1+i%3))
		add(replica1, fp, in, 5*time.Minute, base*float64(1+i%4))
		// Windows on the 3h range's bounds: 7 days back (in) and a second
		// before it (out), the default lookback's start (in it) and a
		// second before it (the newest older window), one that ends at the
		// range's start (in) and one straddling it (out of both).
		if i%3 == 0 {
			week := h3.Add(-7 * 24 * time.Hour)
			day := h3.Add(-24 * time.Hour)
			add(primary, fp, week, 5*time.Minute, base)
			add(primary, fp, week.Add(-time.Second), 5*time.Minute, base*20)
			add(replica1, fp, week, 5*time.Minute, base)
			add(replica2, fp, week.Add(-time.Second), 5*time.Minute, base*20)
			add(primary, fp, day, 5*time.Minute, base)
			add(primary, fp, day.Add(-time.Second), 5*time.Minute, base)
			add(replica1, fp, day.Add(-time.Second), 5*time.Minute, base)
			add(replica2, fp, day.Add(-time.Second), 5*time.Minute, base)
			add(primary, fp, h3.Add(-5*time.Minute), 5*time.Minute, base)
			add(replica1, fp, h3.Add(-time.Minute), 5*time.Minute, base*30)
		}
	}
	// A fingerprint that reaches $8 samples, for the 3h range, only with
	// its window exactly 7 days back: 29 windows before the default
	// lookback, plus that one. On the replica the last window counted has
	// both hosts in it, so it has 31.
	var bound int64
	if err := conn.QueryRow(ctx, `insert into rotten.fingerprints (fingerprint, normalized) values ('oracle-bound', 'select oracle_bound') returning id`).Scan(&bound); err != nil {
		t.Fatal(err)
	}
	d.normalized[bound] = "select oracle_bound"
	d.bound = bound
	exact := func(h host, start time.Time, ms float64) {
		rows = append(rows, []any{bound, h.logical, h.physical, start, start.Add(5 * time.Minute), 1.0, ms})
	}
	week := h3.Add(-7 * 24 * time.Hour)
	for k := range 29 {
		at := h3.Add(-24*time.Hour - time.Second - time.Duration(k)*4*time.Hour)
		exact(primary, at, 10+float64(k%5))
		exact(replica1, at, 10+float64(k%5))
	}
	exact(primary, week, 11)
	exact(primary, week.Add(-time.Second), 500)
	exact(replica1, week, 12)
	exact(replica2, week, 13)
	exact(replica2, week.Add(-time.Second), 500)
	exact(primary, h3.Add(time.Hour), 40)
	exact(replica2, h3.Add(time.Hour), 40)

	if _, err := conn.CopyFrom(ctx, pgx.Identifier{"rotten", "events"},
		[]string{"fingerprint_id", "logical_source_id", "physical_source_id", "observed_window_start", "observed_window_end", "calls", "time"},
		pgx.CopyFromRows(rows)); err != nil {
		t.Fatal(err)
	}

	// Contexts on about a third of the events: a controller#action on most,
	// a job tag on some, and both kinds on a few.
	for _, q := range []string{
		`insert into rotten.controllers (controller) values ('Alpha'), ('Beta') on conflict do nothing`,
		`insert into rotten.actions (action) values ('index'), ('show') on conflict do nothing`,
		`insert into rotten.job_tags (job_tag) values ('Nightly#perform') on conflict do nothing`,
		`insert into rotten.event_context (event_id, observed_window_start, observed_window_end,
		     controller_id, action_id, job_tag_id, c, logical_source_id, attributed_time)
		 select e.id, e.observed_window_start, e.observed_window_end,
		   case when k.job then null else (select id from rotten.controllers where controller = case when e.fingerprint_id % 2 = 0 then 'Alpha' else 'Beta' end) end,
		   case when k.job then null else (select id from rotten.actions where action = case when e.id % 2 = 0 then 'index' else 'show' end) end,
		   case when k.job then (select id from rotten.job_tags where job_tag = 'Nightly#perform') end,
		   1, e.logical_source_id, e.time
		 from rotten.events e
		 join rotten.logical_sources s on s.id = e.logical_source_id and s.project = '` + oracleProject + `'
		 cross join lateral (select n, (e.fingerprint_id + n) % 4 = 0 as job
		                     from generate_series(1, case when e.id % 7 = 0 then 2 else 1 end) n) k
		 where e.id % 3 = 0`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	r, err := conn.Query(ctx, `
		select e.logical_source_id, e.fingerprint_id, e.observed_window_start, e.observed_window_end, e.calls, e.time,
		  coalesce(array_agg(case when ec.controller_id is not null or ec.action_id is not null
		                          then coalesce(c.controller, '') || '#' || coalesce(a.action, '') end)
		           filter (where ec.controller_id is not null or ec.action_id is not null), '{}'),
		  coalesce(array_agg(j.job_tag) filter (where j.job_tag is not null), '{}')
		from rotten.events e
		join rotten.logical_sources s on s.id = e.logical_source_id and s.project = $1
		left join rotten.event_context ec on ec.event_id = e.id and ec.observed_window_start = e.observed_window_start
		left join rotten.controllers c on c.id = ec.controller_id
		left join rotten.actions a on a.id = ec.action_id
		left join rotten.job_tags j on j.id = ec.job_tag_id
		group by e.id, e.logical_source_id, e.fingerprint_id, e.observed_window_start, e.observed_window_end, e.calls, e.time`, oracleProject)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for r.Next() {
		var e oracleEvent
		var actions, jobs []string
		if err := r.Scan(&e.source, &e.fingerprint, &e.start, &e.end, &e.calls, &e.time, &actions, &jobs); err != nil {
			t.Fatal(err)
		}
		e.contexts = append(actions, jobs...)
		d.events = append(d.events, e)
	}
	if err := r.Err(); err != nil {
		t.Fatal(err)
	}
	t.Logf("seeded %d events", len(d.events))
	return d
}

// outliers computes the report from the header's definitions.
func (d *oracleData) outliers(c oracleCase) []oracleRow {
	const minHistory = defaultMinHistory
	var re *regexp.Regexp
	if c.match != "" {
		re = regexp.MustCompile("(?i)" + c.match)
	}
	type key struct {
		source      int
		fingerprint int64
	}
	inRange := map[key]*oracleRow{}
	matched := map[key]bool{}
	var keys []key
	for _, e := range d.events {
		role, ok := d.roles[e.source]
		if !ok || (c.role != "" && role != c.role) || e.calls <= 0 {
			continue
		}
		if e.start.Before(c.start) || !e.start.Before(c.end) || e.end.After(c.end) {
			continue
		}
		k := key{e.source, e.fingerprint}
		r := inRange[k]
		if r == nil {
			r = &oracleRow{source: e.source, fingerprint: e.fingerprint, worst: math.Inf(-1)}
			inRange[k] = r
			keys = append(keys, k)
		}
		r.worst = math.Max(r.worst, e.time/e.calls)
		r.total += e.time
		if re != nil && slices.ContainsFunc(e.contexts, re.MatchString) {
			matched[k] = true
		}
	}

	length := c.end.Sub(c.start)
	lookback := min(max(length, 24*time.Hour), 7*24*time.Hour)
	var out []oracleRow
	for _, k := range keys {
		if re != nil && !matched[k] && !re.MatchString(d.normalized[k.fingerprint]) {
			continue
		}
		type sample struct {
			start time.Time
			ms    float64
		}
		var recent, older []sample
		for _, e := range d.events {
			if e.source != k.source || e.fingerprint != k.fingerprint || e.calls <= 0 ||
				!e.start.Before(c.start) || e.end.After(c.start) {
				continue
			}
			switch {
			case !e.start.Before(c.start.Add(-lookback)):
				recent = append(recent, sample{e.start, e.time / e.calls})
			case !e.start.Before(c.start.Add(-7 * 24 * time.Hour)):
				older = append(older, sample{e.start, e.time / e.calls})
			}
		}
		samples := make([]float64, 0, len(recent))
		for _, s := range recent {
			samples = append(samples, s.ms)
		}
		r := *inRange[k]
		if len(samples) < minHistory && length < 7*24*time.Hour {
			// Newest older windows first, whole windows, until there are
			// enough: a sample counts when fewer than the samples still
			// needed are in strictly newer windows.
			sort.SliceStable(older, func(i, j int) bool { return older[i].start.After(older[j].start) })
			need := minHistory - len(samples)
			for i, s := range older {
				newer := i
				for newer > 0 && older[newer-1].start.Equal(s.start) {
					newer--
				}
				if newer >= need {
					break
				}
				samples = append(samples, s.ms)
				r.extended = true
			}
		}
		if len(samples) < minHistory {
			continue
		}
		r.samples = len(samples)
		r.median = percentileCont(samples, 0.5)
		deviations := make([]float64, len(samples))
		for i, x := range samples {
			deviations[i] = math.Abs(x - r.median)
		}
		mad := percentileCont(deviations, 0.5)
		r.spread = math.Max(math.Max(1.4826*mad, (defaultRatio-1)/c.sigma*r.median), 0.01)
		r.score = (r.worst - r.median) / r.spread
		if r.score > c.sigma {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		switch {
		case a.score != b.score:
			return a.score > b.score
		case a.worst != b.worst:
			return a.worst > b.worst
		case a.total != b.total:
			return a.total > b.total
		case a.fingerprint != b.fingerprint:
			return a.fingerprint < b.fingerprint
		}
		return a.source < b.source
	})
	if len(out) > c.limit {
		out = out[:c.limit]
	}
	return out
}

// percentileCont is Postgres's percentile_cont: linear interpolation
// between the two nearest ranks.
func percentileCont(xs []float64, p float64) float64 {
	s := slices.Clone(xs)
	slices.Sort(s)
	pos := p * float64(len(s)-1)
	lo, hi := math.Floor(pos), math.Ceil(pos)
	if lo == hi {
		return s[int(lo)]
	}
	return s[int(lo)] + (s[int(hi)]-s[int(lo)])*(pos-lo)
}
