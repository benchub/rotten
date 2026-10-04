package devtraffic

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// setupLock is the advisory lock key Setup holds, so two generators starting
// at once don't race to create or seed the same shard.
const setupLock = 7_274_201_004

// Setup creates the app roles, and each shard's database, schema, tables and
// grants if they're missing, and seeds any shard whose users table is empty.
// Shard n is database ShardSchema(n) holding schema ShardSchema(n). It's safe
// to rerun: existing roles keep working (their passwords are reset to the
// role name), and seeded shards are left alone. adminDSN must be a superuser
// or otherwise able to create roles and databases. It returns the smallest
// shard's sizes, read from the data, so a rerun with a different scale still
// picks valid IDs.
func Setup(ctx context.Context, adminDSN string, shards int, scale float64) (Sizes, error) {
	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		return Sizes{}, fmt.Errorf("devtraffic: connect as admin: %w", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, int64(setupLock)); err != nil {
		return Sizes{}, fmt.Errorf("devtraffic: setup lock: %w", err)
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, int64(setupLock))

	for _, role := range []string{WebRole, JobRole} {
		sql := fmt.Sprintf(`DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '%[1]s') THEN CREATE ROLE %[1]s; END IF;
END $$;
ALTER ROLE %[1]s LOGIN PASSWORD '%[1]s'`, role)
		if err := execScript(ctx, conn, sql); err != nil {
			return Sizes{}, fmt.Errorf("devtraffic: role %s: %w", role, err)
		}
	}

	want := SizesFor(scale)
	var got Sizes
	for n := 1; n <= shards; n++ {
		s, err := setupShard(ctx, conn, adminDSN, ShardSchema(n), want)
		if err != nil {
			return Sizes{}, err
		}
		if n == 1 || s.Users < got.Users {
			got.Users = s.Users
		}
		if n == 1 || s.Courses < got.Courses {
			got.Courses = s.Courses
		}
	}
	got.AssignmentsPerCourse = want.AssignmentsPerCourse
	got.PageViews = want.PageViews
	return got, nil
}

// setupShard creates database name if it's missing, then the schema of the
// same name in it, and seeds it if it's empty. It returns the shard's user
// and course counts.
func setupShard(ctx context.Context, admin *pgx.Conn, adminDSN, name string, want Sizes) (Sizes, error) {
	var exists bool
	if err := admin.QueryRow(ctx, `SELECT EXISTS (SELECT FROM pg_database WHERE datname = $1)`, name).Scan(&exists); err != nil {
		return Sizes{}, fmt.Errorf("devtraffic: check database %s: %w", name, err)
	}
	if !exists {
		if _, err := admin.Exec(ctx, `CREATE DATABASE `+name); err != nil {
			return Sizes{}, fmt.Errorf("devtraffic: create database %s: %w", name, err)
		}
	}
	cfg, err := pgx.ParseConfig(adminDSN)
	if err != nil {
		return Sizes{}, fmt.Errorf("devtraffic: parse DSN: %w", err)
	}
	cfg.Database = name
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return Sizes{}, fmt.Errorf("devtraffic: connect to %s: %w", name, err)
	}
	defer conn.Close(context.Background())

	if err := execScript(ctx, conn, strings.ReplaceAll(shardDDL, "{s}", name)); err != nil {
		return Sizes{}, fmt.Errorf("devtraffic: create %s: %w", name, err)
	}
	var seeded bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT FROM `+name+`.users)`).Scan(&seeded); err != nil {
		return Sizes{}, fmt.Errorf("devtraffic: check %s: %w", name, err)
	}
	if !seeded {
		if err := seedShard(ctx, conn, name, want); err != nil {
			return Sizes{}, fmt.Errorf("devtraffic: seed %s: %w", name, err)
		}
	}
	var s Sizes
	if err := conn.QueryRow(ctx, `SELECT (SELECT count(*) FROM `+name+`.users), (SELECT count(*) FROM `+name+`.courses)`).Scan(&s.Users, &s.Courses); err != nil {
		return Sizes{}, fmt.Errorf("devtraffic: size %s: %w", name, err)
	}
	return s, nil
}

// execScript runs a multi-statement script over the simple protocol.
func execScript(ctx context.Context, conn *pgx.Conn, sql string) error {
	_, err := conn.PgConn().Exec(ctx, sql).ReadAll()
	return err
}

const shardDDL = `
CREATE SCHEMA IF NOT EXISTS {s};
CREATE TABLE IF NOT EXISTS {s}.users (
  id bigint PRIMARY KEY,
  name text NOT NULL,
  sortable_name text NOT NULL,
  email text NOT NULL,
  workflow_state text NOT NULL,
  last_seen_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS users_lower_email ON {s}.users (lower(email));
CREATE TABLE IF NOT EXISTS {s}.courses (
  id bigint PRIMARY KEY,
  name text NOT NULL,
  course_code text NOT NULL,
  workflow_state text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS {s}.enrollments (
  id bigserial PRIMARY KEY,
  user_id bigint NOT NULL,
  course_id bigint NOT NULL,
  type text NOT NULL,
  workflow_state text NOT NULL,
  computed_final_score numeric,
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (user_id, course_id)
);
CREATE INDEX IF NOT EXISTS enrollments_course_id ON {s}.enrollments (course_id);
CREATE TABLE IF NOT EXISTS {s}.favorites (
  id bigserial PRIMARY KEY,
  user_id bigint NOT NULL,
  course_id bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (user_id, course_id)
);
CREATE TABLE IF NOT EXISTS {s}.assignments (
  id bigint PRIMARY KEY,
  course_id bigint NOT NULL,
  title text NOT NULL,
  points_possible numeric NOT NULL,
  due_at timestamptz,
  workflow_state text NOT NULL
);
CREATE INDEX IF NOT EXISTS assignments_course_id ON {s}.assignments (course_id);
CREATE TABLE IF NOT EXISTS {s}.submissions (
  id bigserial PRIMARY KEY,
  assignment_id bigint NOT NULL,
  user_id bigint NOT NULL,
  body text,
  score numeric,
  workflow_state text NOT NULL,
  submitted_at timestamptz,
  graded_at timestamptz,
  UNIQUE (assignment_id, user_id)
);
CREATE INDEX IF NOT EXISTS submissions_user_id ON {s}.submissions (user_id);
CREATE TABLE IF NOT EXISTS {s}.page_views (
  id bigserial PRIMARY KEY,
  user_id bigint NOT NULL,
  course_id bigint NOT NULL,
  url text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS page_views_user_created ON {s}.page_views (user_id, created_at);
GRANT USAGE ON SCHEMA {s} TO ` + WebRole + `, ` + JobRole + `;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {s} TO ` + WebRole + `, ` + JobRole + `;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA {s} TO ` + WebRole + `, ` + JobRole + `;
`

// seedShard fills an empty shard in one transaction.
func seedShard(ctx context.Context, conn *pgx.Conn, schema string, sz Sizes) error {
	r := strings.NewReplacer(
		"{s}", schema,
		"{users}", fmt.Sprint(sz.Users),
		"{courses}", fmt.Sprint(sz.Courses),
		"{per}", fmt.Sprint(sz.AssignmentsPerCourse),
		"{page_views}", fmt.Sprint(sz.PageViews),
	)
	return execScript(ctx, conn, r.Replace(seedSQL))
}

const seedSQL = `
BEGIN;
INSERT INTO {s}.users (id, name, sortable_name, email, workflow_state, created_at)
SELECT g, 'Student ' || g, 'Student, ' || lpad(g::text, 6, '0'), 'user' || g || '@example.edu',
       CASE WHEN g % 50 = 0 THEN 'deleted' ELSE 'registered' END, now() - g * interval '1 hour'
  FROM generate_series(1, {users}) g;
INSERT INTO {s}.courses (id, name, course_code, workflow_state)
SELECT g, 'Course ' || g, 'C' || lpad(g::text, 4, '0'),
       CASE WHEN g % 10 = 0 THEN 'completed' ELSE 'available' END
  FROM generate_series(1, {courses}) g;
INSERT INTO {s}.enrollments (user_id, course_id, type, workflow_state)
SELECT u, (u * 7 + k * 13) % {courses} + 1,
       CASE WHEN u % 40 = 0 THEN 'TeacherEnrollment' ELSE 'StudentEnrollment' END,
       CASE WHEN (u + k) % 17 = 0 THEN 'completed' ELSE 'active' END
  FROM generate_series(1, {users}) u, generate_series(0, 2) k
ON CONFLICT DO NOTHING;
INSERT INTO {s}.assignments (id, course_id, title, points_possible, due_at, workflow_state)
SELECT a, (a - 1) / {per} + 1, 'Assignment ' || a, 10 * (1 + a % 10), now() + (a % 30 - 10) * interval '1 day',
       CASE WHEN a % 9 = 0 THEN 'unpublished' ELSE 'published' END
  FROM generate_series(1, {courses} * {per}) a;
INSERT INTO {s}.submissions (assignment_id, user_id, body, score, workflow_state, submitted_at, graded_at)
SELECT a.id, e.user_id, 'essay ' || a.id || '-' || e.user_id,
       CASE WHEN (a.id + e.user_id) % 3 <> 0 THEN round((random() * a.points_possible)::numeric, 1) END,
       CASE WHEN (a.id + e.user_id) % 3 <> 0 THEN 'graded' ELSE 'submitted' END,
       now() - (a.id % 20) * interval '1 day',
       CASE WHEN (a.id + e.user_id) % 3 <> 0 THEN now() - (a.id % 10) * interval '1 day' END
  FROM {s}.enrollments e JOIN {s}.assignments a ON a.course_id = e.course_id
 WHERE (a.id + e.user_id) % 5 <> 0;
INSERT INTO {s}.favorites (user_id, course_id)
SELECT user_id, course_id FROM {s}.enrollments WHERE user_id % 3 = 0;
INSERT INTO {s}.page_views (user_id, course_id, url, created_at)
SELECT 1 + (g * 7919) % {users}, 1 + (g * 31) % {courses}, '/courses/' || (1 + (g * 31) % {courses}),
       now() - (g % 86400) * interval '1 second'
  FROM generate_series(1, {page_views}) g;
COMMIT;
ANALYZE {s}.users, {s}.courses, {s}.enrollments, {s}.favorites, {s}.assignments, {s}.submissions, {s}.page_views;
`
