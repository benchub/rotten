package fingerprint

import (
	"io"
	"log"
	"os"
	"testing"
)

// TestSchemaCollapsing covers both schema modes. The golden file pins the
// default mode. This test also pins that KeepSchemas turns collapsing off.
func TestSchemaCollapsing(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stderr)

	queries := []string{
		"SELECT * FROM users WHERE id = 1",
		"SELECT * FROM public.users WHERE id = 2",
		"SELECT * FROM shard_1.users WHERE id = 3",
		"SELECT * FROM shard_27.users WHERE id = 4",
	}
	fps := func(t *testing.T, opts Options) []string {
		var out []string
		for _, q := range queries {
			fp, err := Normalized(q, opts)
			if err != nil {
				t.Fatalf("Normalized(%q, %+v): %v", q, opts, err)
			}
			out = append(out, fp)
		}
		return out
	}

	t.Run("default collapses", func(t *testing.T) {
		got := fps(t, Options{})
		for i, fp := range got {
			if fp != got[0] {
				t.Errorf("%q = %s, want %s (same as %q)", queries[i], fp, got[0], queries[0])
			}
		}
	})

	t.Run("KeepSchemas keeps them apart", func(t *testing.T) {
		got := fps(t, Options{KeepSchemas: true})
		seen := map[string]string{}
		for i, fp := range got {
			if prev, ok := seen[fp]; ok {
				t.Errorf("%q and %q share fingerprint %s", prev, queries[i], fp)
			}
			seen[fp] = queries[i]
		}
	})

	t.Run("CREATE SCHEMA", func(t *testing.T) {
		for _, tc := range []struct {
			opts Options
			same bool
		}{{Options{}, true}, {Options{KeepSchemas: true}, false}} {
			a, err := Normalized("CREATE SCHEMA shard_1", tc.opts)
			if err != nil {
				t.Fatal(err)
			}
			b, err := Normalized("CREATE SCHEMA shard_27", tc.opts)
			if err != nil {
				t.Fatal(err)
			}
			if (a == b) != tc.same {
				t.Errorf("%+v: shard_1 = %s, shard_27 = %s, want same=%v", tc.opts, a, b, tc.same)
			}
		}
	})

	t.Run("KeepSchemas still collapses literals", func(t *testing.T) {
		a, err := Normalized("SELECT * FROM shard_1.users WHERE id = 1", Options{KeepSchemas: true})
		if err != nil {
			t.Fatal(err)
		}
		b, err := Normalized("SELECT * FROM shard_1.users WHERE id = 99", Options{KeepSchemas: true})
		if err != nil {
			t.Fatal(err)
		}
		if a != b {
			t.Errorf("literals split fingerprints: %s vs %s", a, b)
		}
	})
}

func TestQualifiedNameSchemaCollapsing(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stderr)

	for _, tc := range []struct {
		name    string
		queries []string
	}{
		{
			name: "column refs schema",
			queries: []string{
				"SELECT users.id FROM users",
				"SELECT public.users.id FROM users",
			},
		},
		{
			name: "column refs catalog schema",
			queries: []string{
				"SELECT catalog_1.public.users.id FROM users",
				"SELECT catalog_1.shard_27.users.id FROM users",
			},
		},
		{
			name: "function names",
			queries: []string{
				"SELECT f(id) FROM users",
				"SELECT shard_1.f(id) FROM users",
				"SELECT shard_27.f(id) FROM users",
			},
		},
		{
			name: "drop table names",
			queries: []string{
				"DROP TABLE users",
				"DROP TABLE public.users",
				"DROP TABLE shard_27.users",
			},
		},
		{
			name: "drop function names",
			queries: []string{
				"DROP FUNCTION f(int)",
				"DROP FUNCTION public.f(int)",
				"DROP FUNCTION shard_27.f(int)",
			},
		},
		{
			name: "type names",
			queries: []string{
				"SELECT id::mytype FROM users",
				"SELECT id::public.mytype FROM users",
				"SELECT id::shard_27.mytype FROM users",
			},
		},
		{
			name: "comment type names",
			queries: []string{
				"COMMENT ON TYPE mytype IS 'x'",
				"COMMENT ON TYPE public.mytype IS 'x'",
				"COMMENT ON TYPE shard_27.mytype IS 'x'",
			},
		},
		{
			name: "alter function names",
			queries: []string{
				"ALTER FUNCTION f(int) OWNER TO alice",
				"ALTER FUNCTION public.f(int) OWNER TO alice",
				"ALTER FUNCTION shard_27.f(int) OWNER TO alice",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertSameFingerprint(t, Options{}, tc.queries...)
			assertDifferentFingerprints(t, Options{KeepSchemas: true}, tc.queries...)
		})
	}
}

func TestSQLSyntaxFunctionFingerprintsStayDefault(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stderr)

	for _, q := range []string{
		"SELECT EXTRACT(YEAR FROM TIMESTAMP '2024-01-02')",
		"SELECT TIMESTAMP '2024-01-02 03:04:05' AT TIME ZONE 'UTC'",
		"SELECT TRIM(BOTH 'x' FROM 'xxhelloxx')",
		"SELECT SUBSTRING('abcdef' FROM 2 FOR 3)",
	} {
		defaultFP, err := Normalized(q, Options{})
		if err != nil {
			t.Fatalf("Normalized(%q, default): %v", q, err)
		}
		keepSchemasFP, err := Normalized(q, Options{KeepSchemas: true})
		if err != nil {
			t.Fatalf("Normalized(%q, KeepSchemas): %v", q, err)
		}
		if defaultFP != keepSchemasFP {
			t.Errorf("Normalized(%q, default) = %s, want master-compatible %s", q, defaultFP, keepSchemasFP)
		}
	}
}

func TestObjectListSchemaCollapsingByShape(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stderr)

	for _, tc := range []struct {
		name      string
		same      []string
		different []string
	}{
		{
			name: "drop trigger table part",
			same: []string{
				"DROP TRIGGER trg ON users",
				"DROP TRIGGER trg ON public.users",
			},
			different: []string{
				"DROP TRIGGER trg ON public.users",
				"DROP TRIGGER trg ON public.orders",
			},
		},
		{
			name: "drop policy table part",
			same: []string{
				"DROP POLICY pol ON users",
				"DROP POLICY pol ON public.users",
			},
		},
		{
			name: "drop rule table part",
			same: []string{
				"DROP RULE rul ON users",
				"DROP RULE rul ON public.users",
			},
		},
		{
			name: "comment column table part",
			same: []string{
				"COMMENT ON COLUMN users.id IS 'x'",
				"COMMENT ON COLUMN public.users.id IS 'x'",
			},
			different: []string{
				"COMMENT ON COLUMN users.id IS 'x'",
				"COMMENT ON COLUMN orders.id IS 'x'",
			},
		},
		{
			name: "comment constraint table part",
			same: []string{
				"COMMENT ON CONSTRAINT users_pkey ON users IS 'x'",
				"COMMENT ON CONSTRAINT users_pkey ON public.users IS 'x'",
			},
		},
		{
			name: "drop operator class name part",
			same: []string{
				"DROP OPERATOR CLASS c USING btree",
				"DROP OPERATOR CLASS s.c USING btree",
			},
		},
		{
			name: "drop operator family name part",
			same: []string{
				"DROP OPERATOR FAMILY f USING btree",
				"DROP OPERATOR FAMILY s.f USING btree",
			},
		},
		{
			name: "drop operator name",
			same: []string{
				"DROP OPERATOR a.+(int, int)",
				"DROP OPERATOR b.+(int, int)",
			},
		},
		{
			name: "comment operator name",
			same: []string{
				"COMMENT ON OPERATOR a.+(int, int) IS 'x'",
				"COMMENT ON OPERATOR b.+(int, int) IS 'x'",
			},
		},
		{
			name: "alter operator name",
			same: []string{
				"ALTER OPERATOR a.+(int,int) SET (RESTRICT = eqsel)",
				"ALTER OPERATOR b.+(int,int) SET (RESTRICT = eqsel)",
			},
		},
		// CAST and TRANSFORM object lists contain TypeName nodes, and
		// COMMENT ON CONSTRAINT ... ON DOMAIN stores the domain as a TypeName.
		// The TypeName walker collapses those schemas; these cases lock that
		// in without object-list-specific branches.
		{
			name: "drop cast type names",
			same: []string{
				"DROP CAST (a.t AS b.t)",
				"DROP CAST (c.t AS d.t)",
			},
		},
		{
			name: "comment cast type names",
			same: []string{
				"COMMENT ON CAST (a.t AS b.t) IS 'x'",
				"COMMENT ON CAST (c.t AS d.t) IS 'x'",
			},
		},
		{
			name: "drop transform type name",
			same: []string{
				"DROP TRANSFORM FOR a.t LANGUAGE plpgsql",
				"DROP TRANSFORM FOR b.t LANGUAGE plpgsql",
			},
		},
		{
			name: "comment transform type name",
			same: []string{
				"COMMENT ON TRANSFORM FOR a.t LANGUAGE plpgsql IS 'x'",
				"COMMENT ON TRANSFORM FOR b.t LANGUAGE plpgsql IS 'x'",
			},
		},
		{
			name: "domain constraint rename domain name",
			same: []string{
				"ALTER DOMAIN a.d RENAME CONSTRAINT d_check TO d_check_new",
				"ALTER DOMAIN b.d RENAME CONSTRAINT d_check TO d_check_new",
			},
		},
		{
			name: "comment domain constraint domain name",
			same: []string{
				"COMMENT ON CONSTRAINT d_check ON DOMAIN a.d IS 'x'",
				"COMMENT ON CONSTRAINT d_check ON DOMAIN b.d IS 'x'",
			},
		},
		{
			name: "alter collation refresh name",
			same: []string{
				"ALTER COLLATION a.c REFRESH VERSION",
				"ALTER COLLATION b.c REFRESH VERSION",
			},
		},
		{
			name: "alter conversion rename name",
			same: []string{
				"ALTER CONVERSION a.c RENAME TO c2",
				"ALTER CONVERSION b.c RENAME TO c2",
			},
		},
		{
			name: "alter conversion owner name",
			same: []string{
				"ALTER CONVERSION a.c OWNER TO alice",
				"ALTER CONVERSION b.c OWNER TO alice",
			},
		},
		{
			name: "alter conversion set schema old and new schema",
			same: []string{
				"ALTER CONVERSION a.c SET SCHEMA x",
				"ALTER CONVERSION b.c SET SCHEMA y",
			},
		},
		{
			name: "expression operator syntax is not an object list",
			different: []string{
				"SELECT 1 OPERATOR(a.+) 2",
				"SELECT 1 OPERATOR(b.+) 2",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if len(tc.same) > 0 {
				assertSameFingerprint(t, Options{}, tc.same...)
			}
			if len(tc.different) > 0 {
				assertDifferentFingerprints(t, Options{}, tc.different...)
			}
			if len(tc.same) > 0 {
				assertDifferentFingerprints(t, Options{KeepSchemas: true}, tc.same...)
			}
		})
	}
}

func TestAlterSetSchemaCollapsesNewSchema(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stderr)

	same := []string{
		"ALTER TABLE users SET SCHEMA tenant_1",
		"ALTER TABLE users SET SCHEMA tenant_27",
	}
	assertSameFingerprint(t, Options{}, same...)
	assertDifferentFingerprints(t, Options{KeepSchemas: true}, same...)
}

func TestPctTypeSchemaCollapsing(t *testing.T) {
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stderr)

	for _, tc := range []struct {
		name      string
		same      []string
		different []string
	}{
		{
			name: "argument",
			same: []string{
				"CREATE FUNCTION f(x users.id%TYPE) RETURNS int LANGUAGE sql AS 'SELECT 1'",
				"CREATE FUNCTION f(x public.users.id%TYPE) RETURNS int LANGUAGE sql AS 'SELECT 1'",
			},
			different: []string{
				"CREATE FUNCTION f(x users.id%TYPE) RETURNS int LANGUAGE sql AS 'SELECT 1'",
				"CREATE FUNCTION f(x orders.id%TYPE) RETURNS int LANGUAGE sql AS 'SELECT 1'",
			},
		},
		{
			name: "returns",
			same: []string{
				"CREATE FUNCTION f() RETURNS users.id%TYPE LANGUAGE sql AS 'SELECT 1'",
				"CREATE FUNCTION f() RETURNS public.users.id%TYPE LANGUAGE sql AS 'SELECT 1'",
			},
			different: []string{
				"CREATE FUNCTION f() RETURNS users.id%TYPE LANGUAGE sql AS 'SELECT 1'",
				"CREATE FUNCTION f() RETURNS orders.id%TYPE LANGUAGE sql AS 'SELECT 1'",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertSameFingerprint(t, Options{}, tc.same...)
			assertDifferentFingerprints(t, Options{}, tc.different...)
			assertDifferentFingerprints(t, Options{KeepSchemas: true}, tc.same...)
		})
	}
}

func assertSameFingerprint(t *testing.T, opts Options, queries ...string) {
	t.Helper()
	var want string
	for i, q := range queries {
		fp, err := Normalized(q, opts)
		if err != nil {
			t.Fatalf("Normalized(%q, %+v): %v", q, opts, err)
		}
		if i == 0 {
			want = fp
			continue
		}
		if fp != want {
			t.Errorf("Normalized(%q, %+v) = %s, want %s", q, opts, fp, want)
		}
	}
}

func assertDifferentFingerprints(t *testing.T, opts Options, queries ...string) {
	t.Helper()
	seen := map[string]string{}
	for _, q := range queries {
		fp, err := Normalized(q, opts)
		if err != nil {
			t.Fatalf("Normalized(%q, %+v): %v", q, opts, err)
		}
		if prev, ok := seen[fp]; ok {
			t.Errorf("Normalized(%q, %+v) matched %q: %s", q, opts, prev, fp)
		}
		seen[fp] = q
	}
}
