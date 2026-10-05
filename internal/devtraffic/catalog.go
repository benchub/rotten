// Package devtraffic is the dev stack's traffic generator: a made-up LMS
// schema in a few shard databases on the observed server, and a steady load
// of application-like queries that carry production-style marginalia
// comments, so rotten has
// controllers, actions, job tags and a spread of fingerprints to show.
//
// pg_stat_statements keeps one query text per (userid, dbid, toplevel,
// queryid) entry: the first one it saw. On Postgres 18 that text no longer
// includes leading comments, so the generator appends its comments there
// unless told otherwise (see Position). Comments don't change the queryid,
// so the worker attributes every call of an entry to the context in that
// first text. The generator gets several contexts per fingerprint by giving
// each shape several entries that rotten's fingerprint merges back together
// (one per shard database and role, plus VALUES lists of varying length),
// and by choosing which context each entry sees first. Shard schemas alone
// wouldn't do: Postgres 18 builds the queryid from relation names, not OIDs,
// so the same table name in two schemas of one database is one entry. See
// dev/README.md.
package devtraffic

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
)

// The app roles the generator connects as. Setup creates them.
const (
	WebRole = "lms_web"
	JobRole = "lms_jobs"
)

// ShardSchema is the name of shard n (1-based): both its database and the
// schema in it that holds its tables.
func ShardSchema(n int) string { return "lms_shard_" + strconv.Itoa(n) }

// Sizes are the seeded row counts in each shard. IDs are 1..Users,
// 1..Courses, and 1..Assignments, with AssignmentsPerCourse assignments per
// course in course order.
type Sizes struct {
	Users, Courses, AssignmentsPerCourse, PageViews int
}

// Assignments is the number of assignments in a shard.
func (s Sizes) Assignments() int { return s.Courses * s.AssignmentsPerCourse }

// SizesFor scales the default shard sizes (2000 users, 100 courses) by
// scale, with small floors so a tiny scale still has data.
func SizesFor(scale float64) Sizes {
	n := func(base, floor int) int { return max(floor, int(float64(base)*scale)) }
	return Sizes{
		Users:                n(2000, 50),
		Courses:              n(100, 5),
		AssignmentsPerCourse: 8,
		PageViews:            n(20000, 500),
	}
}

// Context is a controller and action (a web request) or a job tag (a
// background job), and the shapes one request or job runs, in order.
type Context struct {
	Controller, Action string
	JobTag             string
	Weight             int
	// SplitReplicaPercent is the percentage of this context's runs of
	// Split shapes that go to the replica. See ReplicaPercent.
	SplitReplicaPercent int
	Shapes              []string
}

// IsJob reports whether c is a background job rather than a web request.
func (c Context) IsJob() bool { return c.JobTag != "" }

// Label names c for logs and test output.
func (c Context) Label() string {
	if c.IsJob() {
		return c.JobTag
	}
	return c.Controller + "#" + c.Action
}

// Meta is one request's or job's marginalia: a fresh context ID, and a
// hostname and pid from the pool.
type Meta struct {
	ContextID string
	Hostname  string
	PID       int
}

// Comment is c's marginalia comment for m, keys in alphabetical order like
// production's.
func Comment(c Context, m Meta) string {
	if c.IsJob() {
		return fmt.Sprintf("/*context_id:%s,hostname:%s,job_tag:%s,pid:%d*/", m.ContextID, m.Hostname, c.JobTag, m.PID)
	}
	return fmt.Sprintf("/*action:%s,context_id:%s,controller:%s,hostname:%s,pid:%d*/", c.Action, m.ContextID, c.Controller, m.Hostname, m.PID)
}

// Position is where a statement's comment goes.
type Position string

const (
	// Leading puts the comment first, as production does. Postgres 18's
	// pg_stat_statements drops leading comments from the text it keeps, so
	// on 18 the worker never sees them.
	Leading Position = "leading"
	// Trailing puts the comment last, where every supported version keeps
	// it.
	Trailing Position = "trailing"
	// Auto is Leading before Postgres 18 and Trailing on 18 and later.
	Auto Position = "auto"
)

// PositionFor resolves Auto for a server_version_num.
func PositionFor(p Position, serverVersionNum int) Position {
	if p != Auto {
		return p
	}
	if serverVersionNum >= 180000 {
		return Trailing
	}
	return Leading
}

type host struct {
	name string
	pids []int
}

// HostPool is the small, fixed set of app and job hosts and their pids.
type HostPool struct {
	web, job []host
}

// NewHostPool picks 6 app hosts and 4 job hosts with 3 pids each.
func NewHostPool(r *rand.Rand) *HostPool {
	mk := func(format string, first, n int) []host {
		hosts := make([]host, n)
		for i := range hosts {
			hosts[i].name = fmt.Sprintf(format, first+i)
			for range 3 {
				hosts[i].pids = append(hosts[i].pids, 1000+r.IntN(4_000_000))
			}
		}
		return hosts
	}
	return &HostPool{
		web: mk("app0100012202%02d", 10, 6),
		job: mk("job0100010452%02d", 1, 4),
	}
}

// NewRequest returns fresh metadata for one request or job in c: a random
// UUID (web) or 12-digit number (job) context ID, and a host and pid from
// the pool.
func (p *HostPool) NewRequest(r *rand.Rand, c Context) Meta {
	hosts, id := p.web, uuid4(r)
	if c.IsJob() {
		hosts, id = p.job, strconv.FormatInt(100_000_000_000+r.Int64N(900_000_000_000), 10)
	}
	h := hosts[r.IntN(len(hosts))]
	return Meta{ContextID: id, Hostname: h.name, PID: h.pids[r.IntN(len(h.pids))]}
}

func uuid4(r *rand.Rand) string {
	var b [16]byte
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// Shape is one query shape: one fingerprint. The shapes table and the
// contexts table together say, per shape, its SQL (build), which contexts
// run it (RunBy), whether it only reads (ReadOnly), and where it runs when
// there's a replica (Route, with the contexts' SplitReplicaPercent).
type Shape struct {
	Name string
	// ReadOnly is true if the shape runs in a read-only transaction, as on
	// a hot standby. Only read-only shapes may route to the replica.
	ReadOnly bool
	// Route is where the shape runs when there's a replica; empty means
	// OnPrimary.
	Route Route
	// build returns the SQL, with {s} standing for the shard schema, and its
	// arguments, during episode ep.
	build func(r *rand.Rand, sz Sizes, ep Episode) (string, []any)
}

// RunBy lists the contexts that run s, in contexts order.
func (s Shape) RunBy() []Context {
	var out []Context
	for _, c := range contexts {
		if slices.Contains(c.Shapes, s.Name) {
			out = append(out, c)
		}
	}
	return out
}

// Render returns the statement for one run of s in context c on schema, with
// c's comment for m at pos (Leading or Trailing), and its arguments, outside
// any episode.
func (s Shape) Render(c Context, m Meta, schema string, pos Position, r *rand.Rand, sz Sizes) (string, []any) {
	return s.RenderIn(c, m, schema, pos, r, sz, NoEpisode)
}

// RenderIn is Render during episode ep. Episodes change arguments, never the
// SQL, so a shape keeps its fingerprint.
func (s Shape) RenderIn(c Context, m Meta, schema string, pos Position, r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
	sql, args := s.build(r, sz, ep)
	sql = strings.ReplaceAll(sql, "{s}", schema)
	if pos == Trailing {
		return sql + " " + Comment(c, m), args
	}
	return Comment(c, m) + " " + sql, args
}

func userID(r *rand.Rand, sz Sizes) int64       { return 1 + r.Int64N(int64(sz.Users)) }
func courseID(r *rand.Rand, sz Sizes) int64     { return 1 + r.Int64N(int64(sz.Courses)) }
func assignmentID(r *rand.Rand, sz Sizes) int64 { return 1 + r.Int64N(int64(sz.Assignments())) }

// idList is an inline integer IN list, as ActiveRecord writes them, of 1 to
// max IDs below n.
func idList(r *rand.Rand, n, maxLen int) string {
	ids := make([]string, 1+r.IntN(maxLen))
	for i := range ids {
		ids[i] = strconv.Itoa(1 + r.IntN(n))
	}
	return strings.Join(ids, ", ")
}

// episodeSleep is course_activity's pause: none, except 0.3 to 0.6 seconds
// in a SlowSleep episode.
func episodeSleep(r *rand.Rand, ep Episode) float64 {
	if ep == SlowSleep {
		return 0.3 + 0.3*r.Float64()
	}
	return 0
}

var shapes = []Shape{
	{Name: "user_by_id", ReadOnly: true, Route: Split, build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `SELECT id, name, sortable_name, email, workflow_state, last_seen_at FROM {s}.users WHERE id = $1 LIMIT 1`, []any{userID(r, sz)}
	}},
	{Name: "user_by_email", ReadOnly: true, build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `SELECT id, name, email, workflow_state FROM {s}.users WHERE lower(email) = lower($1) LIMIT 1`,
			[]any{fmt.Sprintf("user%d@example.edu", userID(r, sz))}
	}},
	{Name: "touch_user", build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		id := userID(r, sz)
		if ep == LockWait {
			id = 1 + r.Int64N(HotUsers)
		}
		return `UPDATE {s}.users SET last_seen_at = now() WHERE id = $1`, []any{id}
	}},
	{Name: "users_in_list", ReadOnly: true, Route: Split, build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `SELECT id, name, sortable_name FROM {s}.users WHERE id IN (` + idList(r, sz.Users, 20) + `) ORDER BY sortable_name`, nil
	}},
	{Name: "search_users", ReadOnly: true, Route: OnReplica, build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `SELECT id, name, email FROM {s}.users WHERE name ILIKE $1 OR email ILIKE $1 ORDER BY sortable_name LIMIT 20`,
			[]any{fmt.Sprintf("%%%d%%", r.IntN(100))}
	}},
	{Name: "course_by_id", ReadOnly: true, Route: Split, build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `SELECT id, name, course_code, workflow_state FROM {s}.courses WHERE id = $1 LIMIT 1`, []any{courseID(r, sz)}
	}},
	{Name: "courses_in_list", ReadOnly: true, Route: Split, build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `SELECT id, name, course_code FROM {s}.courses WHERE id IN (` + idList(r, sz.Courses, 12) + `) AND workflow_state = 'available'`, nil
	}},
	{Name: "courses_for_user", ReadOnly: true, Route: Split, build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `SELECT c.id, c.name, c.course_code, e.type FROM {s}.courses c JOIN {s}.enrollments e ON e.course_id = c.id WHERE e.user_id = $1 AND e.workflow_state = 'active' ORDER BY c.name`,
			[]any{userID(r, sz)}
	}},
	{Name: "favorites_for_user", ReadOnly: true, build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `SELECT id, course_id, created_at FROM {s}.favorites WHERE user_id = $1 ORDER BY created_at`, []any{userID(r, sz)}
	}},
	{Name: "favorite_courses", ReadOnly: true, Route: Split, build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `SELECT c.id, c.name, c.course_code FROM {s}.courses c JOIN {s}.favorites f ON f.course_id = c.id WHERE f.user_id = $1 AND c.workflow_state = 'available' ORDER BY c.name`,
			[]any{userID(r, sz)}
	}},
	{Name: "insert_favorite", build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `INSERT INTO {s}.favorites (user_id, course_id, created_at) VALUES ($1, $2, now()) ON CONFLICT (user_id, course_id) DO NOTHING`,
			[]any{userID(r, sz), courseID(r, sz)}
	}},
	{Name: "delete_favorite", build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `DELETE FROM {s}.favorites WHERE user_id = $1 AND course_id = $2`, []any{userID(r, sz), courseID(r, sz)}
	}},
	{Name: "enrollments_for_course", ReadOnly: true, Route: Split, build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `SELECT e.id, e.type, e.workflow_state, u.id, u.sortable_name FROM {s}.enrollments e JOIN {s}.users u ON u.id = e.user_id WHERE e.course_id = $1 AND e.workflow_state <> 'deleted' ORDER BY u.sortable_name LIMIT 50`,
			[]any{courseID(r, sz)}
	}},
	{Name: "enrollment_counts", ReadOnly: true, Route: OnReplica, build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `SELECT type, workflow_state, count(*) FROM {s}.enrollments WHERE course_id = $1 GROUP BY type, workflow_state`, []any{courseID(r, sz)}
	}},
	{Name: "recompute_final_scores", build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `UPDATE {s}.enrollments e SET computed_final_score = sub.score, updated_at = now() FROM (SELECT s.user_id, avg(s.score) AS score FROM {s}.submissions s JOIN {s}.assignments a ON a.id = s.assignment_id WHERE a.course_id = $1 AND s.score IS NOT NULL GROUP BY s.user_id) sub WHERE e.course_id = $1 AND e.user_id = sub.user_id`,
			[]any{courseID(r, sz)}
	}},
	{Name: "assignments_for_course", ReadOnly: true, Route: Split, build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `SELECT id, title, points_possible, due_at FROM {s}.assignments WHERE course_id = $1 AND workflow_state = 'published' ORDER BY due_at NULLS LAST`,
			[]any{courseID(r, sz)}
	}},
	{Name: "assignment_by_id", ReadOnly: true, Route: Split, build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `SELECT id, course_id, title, points_possible, due_at, workflow_state FROM {s}.assignments WHERE id = $1 LIMIT 1`, []any{assignmentID(r, sz)}
	}},
	{Name: "submissions_for_assignment", ReadOnly: true, Route: Split, build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `SELECT s.id, s.user_id, s.score, s.workflow_state, u.sortable_name FROM {s}.submissions s JOIN {s}.users u ON u.id = s.user_id WHERE s.assignment_id = $1 ORDER BY u.sortable_name`,
			[]any{assignmentID(r, sz)}
	}},
	{Name: "submission_for_user", ReadOnly: true, build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `SELECT id, body, score, workflow_state, submitted_at, graded_at FROM {s}.submissions WHERE assignment_id = $1 AND user_id = $2`,
			[]any{assignmentID(r, sz), userID(r, sz)}
	}},
	{Name: "upsert_submission", build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `INSERT INTO {s}.submissions (assignment_id, user_id, body, workflow_state, submitted_at) VALUES ($1, $2, $3, 'submitted', now()) ON CONFLICT (assignment_id, user_id) DO UPDATE SET body = excluded.body, workflow_state = 'submitted', submitted_at = now()`,
			[]any{assignmentID(r, sz), userID(r, sz), fmt.Sprintf("essay draft %d", r.IntN(1_000_000))}
	}},
	{Name: "grade_submission", build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `UPDATE {s}.submissions SET score = $1, workflow_state = 'graded', graded_at = now() WHERE assignment_id = $2 AND user_id = $3`,
			[]any{float64(r.IntN(101)), assignmentID(r, sz), userID(r, sz)}
	}},
	{Name: "course_grade_summary", ReadOnly: true, Route: OnReplica, build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `SELECT a.id, count(s.id), avg(s.score), max(s.score) FROM {s}.assignments a LEFT JOIN {s}.submissions s ON s.assignment_id = a.id WHERE a.course_id = $1 GROUP BY a.id ORDER BY a.id`,
			[]any{courseID(r, sz)}
	}},
	// Slow only in a SlowSleep episode: see episodeSleep.
	{Name: "course_activity", ReadOnly: true, Route: Split, build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `WITH pause AS (SELECT pg_sleep($2)) SELECT count(DISTINCT pv.user_id), count(*) FROM {s}.page_views pv, pause WHERE pv.course_id = $1 AND pv.created_at > now() - interval '7 days'`,
			[]any{courseID(r, sz), episodeSleep(r, ep)}
	}},
	{Name: "insert_page_view", build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `INSERT INTO {s}.page_views (user_id, course_id, url, created_at) VALUES ($1, $2, $3, now()) RETURNING id`,
			[]any{userID(r, sz), courseID(r, sz), fmt.Sprintf("/courses/%d", courseID(r, sz))}
	}},
	// A multi-row VALUES of 2 to 10 rows: each length is its own queryid.
	{Name: "bulk_insert_page_views", build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		n := 2 + r.IntN(9)
		rows := make([]string, n)
		args := make([]any, 0, 3*n)
		for i := range rows {
			rows[i] = fmt.Sprintf("($%d, $%d, $%d, now())", 3*i+1, 3*i+2, 3*i+3)
			c := courseID(r, sz)
			args = append(args, userID(r, sz), c, fmt.Sprintf("/courses/%d/assignments", c))
		}
		return `INSERT INTO {s}.page_views (user_id, course_id, url, created_at) VALUES ` + strings.Join(rows, ", "), args
	}},
	{Name: "recent_page_views", ReadOnly: true, Route: Split, build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `SELECT url, course_id, created_at FROM {s}.page_views WHERE user_id = $1 ORDER BY created_at DESC LIMIT 20`, []any{userID(r, sz)}
	}},
	// Deliberately slow: a sequential scan and three aggregates over a day
	// of page views.
	{Name: "page_view_report", ReadOnly: true, Route: OnReplica, build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `SELECT date_trunc('hour', created_at) AS hour, count(*), count(DISTINCT user_id), count(DISTINCT course_id), count(DISTINCT url) FROM {s}.page_views WHERE created_at > now() - $1::interval GROUP BY 1 ORDER BY 1`,
			[]any{"1 day"}
	}},
	// About 6000 rows and 1 MB at scale 1. The generator reads it slowly in
	// a SlowRead episode.
	{Name: ExportShape, ReadOnly: true, Route: OnReplica, build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `SELECT u.id, u.name, u.sortable_name, u.email, c.name, c.course_code, e.type, e.workflow_state, e.computed_final_score FROM {s}.enrollments e JOIN {s}.users u ON u.id = e.user_id JOIN {s}.courses c ON c.id = e.course_id ORDER BY c.id, u.sortable_name`, nil
	}},
	// The LockWait episode's holder runs this in a transaction it keeps
	// open, so touch_user calls on the hot users wait.
	{Name: LockShape, build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `UPDATE {s}.users SET sortable_name = sortable_name WHERE id <= $1`, []any{int64(HotUsers)}
	}},
	{Name: "prune_page_views", build: func(r *rand.Rand, sz Sizes, ep Episode) (string, []any) {
		return `DELETE FROM {s}.page_views WHERE id IN (SELECT id FROM {s}.page_views WHERE created_at < now() - interval '1 day' ORDER BY id LIMIT 500)`, nil
	}},
}

// lockHolder is the job that holds the hot users' row locks in a LockWait
// episode.
var lockHolder = Context{JobTag: "SisImport.process_users", Weight: 0, Shapes: []string{LockShape}}

var contexts = []Context{
	{Controller: "favorites", Action: "list_favorite_courses", Weight: 5, SplitReplicaPercent: 70, Shapes: []string{"user_by_id", "favorites_for_user", "favorite_courses", "courses_in_list"}},
	{Controller: "favorites", Action: "create", Weight: 2, Shapes: []string{"user_by_id", "course_by_id", "insert_favorite", "favorites_for_user"}},
	{Controller: "favorites", Action: "destroy", Weight: 1, Shapes: []string{"user_by_id", "delete_favorite"}},
	{Controller: "courses", Action: "index", Weight: 4, SplitReplicaPercent: 50, Shapes: []string{"user_by_id", "courses_for_user", "favorites_for_user", "courses_in_list"}},
	{Controller: "courses", Action: "show", Weight: 5, SplitReplicaPercent: 30, Shapes: []string{"user_by_id", "course_by_id", "assignments_for_course", "recent_page_views", "insert_page_view"}},
	{Controller: "users", Action: "dashboard", Weight: 6, SplitReplicaPercent: 20, Shapes: []string{"user_by_id", "touch_user", "courses_for_user", "favorite_courses", "recent_page_views", "insert_page_view"}},
	{Controller: "users", Action: "search", Weight: 1, SplitReplicaPercent: 100, Shapes: []string{"search_users", "users_in_list"}},
	{Controller: "login", Action: "create", Weight: 2, Shapes: []string{"user_by_email", "touch_user", "insert_page_view"}},
	{Controller: "enrollments_api", Action: "index", Weight: 2, SplitReplicaPercent: 90, Shapes: []string{"course_by_id", "enrollments_for_course", "users_in_list", "enrollment_counts"}},
	{Controller: "assignments", Action: "show", Weight: 4, SplitReplicaPercent: 40, Shapes: []string{"user_by_id", "assignment_by_id", "course_by_id", "submission_for_user", "insert_page_view"}},
	{Controller: "submissions", Action: "create", Weight: 2, Shapes: []string{"user_by_id", "assignment_by_id", "upsert_submission", "submission_for_user"}},
	{Controller: "gradebooks", Action: "show", Weight: 2, SplitReplicaPercent: 80, Shapes: []string{"course_by_id", "assignments_for_course", "users_in_list", "course_grade_summary", "course_activity"}},
	{Controller: "gradebooks", Action: "export", Weight: 1, SplitReplicaPercent: 100, Shapes: []string{"course_by_id", ExportShape}},
	{Controller: "gradebooks", Action: "update_submission", Weight: 2, Shapes: []string{"assignment_by_id", "grade_submission", "submission_for_user"}},

	{JobTag: "Enrollment.recompute_final_score", Weight: 3, Shapes: []string{"course_by_id", "course_grade_summary", "recompute_final_scores", "enrollment_counts"}},
	{JobTag: "Submission.auto_grade", Weight: 2, SplitReplicaPercent: 50, Shapes: []string{"assignment_by_id", "submissions_for_assignment", "grade_submission"}},
	{JobTag: "PageView.flush_buffer", Weight: 3, Shapes: []string{"bulk_insert_page_views"}},
	{JobTag: "PageView.prune", Weight: 1, Shapes: []string{"prune_page_views"}},
	{JobTag: "Reports::CourseActivity.generate", Weight: 1, SplitReplicaPercent: 100, Shapes: []string{"page_view_report", "course_activity", "enrollment_counts"}},
	{JobTag: "Favorite.cleanup_concluded", Weight: 1, Shapes: []string{"favorites_for_user", "courses_in_list", "delete_favorite"}},
	{JobTag: "User.touch_last_seen", Weight: 1, Shapes: []string{"users_in_list", "touch_user"}},
	{JobTag: "Reports::GradeExport.generate", Weight: 2, SplitReplicaPercent: 100, Shapes: []string{"courses_in_list", ExportShape}},
	// Weight 0: only the LockWait episode runs it (and warmup, once).
	lockHolder,
	{JobTag: "Course.sync_enrollments", Weight: 1, SplitReplicaPercent: 60, Shapes: []string{"course_by_id", "enrollments_for_course", "users_in_list", "user_by_id"}},
}

// Shapes returns the query shapes.
func Shapes() []Shape { return shapes }

// Contexts returns the web and job contexts.
func Contexts() []Context { return contexts }

// ShapeByName returns the shape called name.
func ShapeByName(name string) (Shape, bool) {
	for _, s := range shapes {
		if s.Name == name {
			return s, true
		}
	}
	return Shape{}, false
}
