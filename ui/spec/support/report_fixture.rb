require "pg"

# The report fixture from internal/testdb/seed_reports.go, ported so the UI
# runs the reports against the same data the Go report tests use. Every
# window sits at an offset before Anchor, which is now() truncated to the
# minute at seed time: "recent" windows are well inside the last 3 hours and
# "old" ones are 4 hours or more back.
#
# The UI connects as rotten_ui, which can only read the event tables, so the
# fixture writes through ROTTEN_UI_TEST_SEED_DATABASE_URL, a rotten_owner
# connection that `make test-ui` provides.
module ReportFixture
  PRIMARY = "primary".freeze
  REPLICA = "replica".freeze
  ENVIRONMENT = "production".freeze
  WINDOW = 10 * 60

  Source = Data.define(:key, :project, :cluster, :role)
  Fingerprint = Data.define(:key, :fingerprint, :normalized)
  Context = Data.define(:controller, :action, :job_tag, :c)
  Event = Data.define(:source, :fingerprint, :start_ago, :calls, :time, :contexts)
  Stat = Data.define(:fingerprint, :source, :type, :count, :mean, :deviation, :last)

  SOURCES = [
    Source.new("canvas13p", "canvas", "13", PRIMARY),
    Source.new("canvas13r", "canvas", "13", REPLICA),
    Source.new("canvas7p", "canvas", "7", PRIMARY),
    Source.new("canvas7r", "canvas", "7", REPLICA),
    Source.new("bridge13p", "bridge", "13", PRIMARY)
  ].freeze

  FINGERPRINTS = [
    Fingerprint.new("users", "select * from users where id = 1", "select * from users where id = $1"),
    Fingerprint.new("courses", "select * from courses where account_id = 1", "select * from courses where account_id = $1"),
    Fingerprint.new("jobs", "update delayed_jobs set locked_by = 'w' where id = 1", "update delayed_jobs set locked_by = $1 where id = $2"),
    Fingerprint.new("slow", "select * from submissions where assignment_id = 1", "select * from submissions where assignment_id = $1")
  ].freeze

  def self.ctx(controller, action, job_tag, c) = Context.new(controller, action, job_tag, c)

  M = 60
  H = 3600

  EVENTS = [
    # canvas 13 primary, recent.
    Event.new("canvas13p", "users", 30 * M, 500, 250, [
      ctx("users", "show", nil, 200), ctx("users", "index", nil, 120), ctx("courses", "show", nil, 80),
      ctx("grades", "index", nil, 50), ctx("api", "list", nil, 40), ctx("login", "new", nil, 1)
    ]),
    Event.new("canvas13p", "users", 50 * M, 100, 50, [ctx("users", "show", nil, 100)]),
    Event.new("canvas13p", "users", 90 * M, 300, 150, [ctx("users", "show", nil, 300)]),
    Event.new("canvas13p", "jobs", 60 * M, 30, 3000, [ctx(nil, nil, "SendEmail", 30)]),
    Event.new("canvas13p", "jobs", 80 * M, 30, 1000, [ctx(nil, nil, "Reindex", 30)]),
    Event.new("canvas13p", "courses", 120 * M, 100, 300, [ctx("courses", "index", nil, 100)]),
    # canvas 13 replica, recent.
    Event.new("canvas13r", "users", 45 * M, 200, 80, [ctx("grades", "show", nil, 200)]),
    Event.new("canvas13r", "jobs", 60 * M, 10, 900, [ctx(nil, nil, "Reindex", 10)]),
    Event.new("canvas13r", "jobs", 110 * M, 15, 300, [ctx(nil, nil, "ReplicaReport", 15)]),
    # canvas 13, old.
    Event.new("canvas13p", "users", 4 * H, 800, 400, [ctx("users", "show", nil, 800)]),
    Event.new("canvas13p", "courses", 4 * H, 9000, 90_000, [ctx("courses", "index", nil, 9000)]),
    Event.new("canvas13r", "users", 26 * H, 7000, 7000, [ctx("grades", "show", nil, 7000)]),
    # canvas 7, recent.
    Event.new("canvas7p", "slow", 40 * M, 20, 800, [ctx("submissions", "index", nil, 20)]),
    Event.new("canvas7p", "users", 50 * M, 60, 30, [ctx("users", "show", nil, 60)]),
    Event.new("canvas7r", "users", 50 * M, 140, 70, [ctx("users", "show", nil, 140)]),
    # bridge 13, recent and old.
    Event.new("bridge13p", "courses", 20 * M, 75, 150, [ctx("programs", "show", nil, 75)]),
    Event.new("bridge13p", "users", 100 * M, 25, 10, [ctx(nil, nil, "SyncLearners", 25)]),
    Event.new("bridge13p", "users", 5 * H, 1000, 400, [])
  ].freeze

  STATS = [
    Stat.new("slow", nil, "mean_time", 1001, 5.034965034965035, 1.4908977911903367, 5),
    Stat.new("slow", "canvas7p", "mean_time", 801, 8.039950062421973, 1.4447800862079763, 8),
    Stat.new("users", nil, "mean_time", 50_000, 0.5, 0.2, 0),
    Stat.new("users", "canvas13p", "mean_time", 30_000, 0.5, 0.1, 0),
    Stat.new("slow", nil, "calls", 1000, 20, 3, 20)
  ].freeze

  Seeded = Data.define(:anchor, :fingerprint_ids)

  def self.connect
    url = ENV.fetch("ROTTEN_UI_TEST_SEED_DATABASE_URL") do
      raise "ROTTEN_UI_TEST_SEED_DATABASE_URL must point at a rotten_owner connection (make test-ui sets it)"
    end
    conn = PG.connect(url)
    conn.exec("set search_path to rotten, public")
    conn
  end

  # Empties the event tables. Logical source 0, the all-sources row, stays.
  def self.clear(conn)
    conn.exec(<<~SQL)
      delete from rotten.event_context;
      delete from rotten.events;
      delete from rotten.fingerprint_stats;
      delete from rotten.ingested_batches;
      delete from rotten.logical_physical_sources;
      delete from rotten.fingerprints;
      delete from rotten.logical_sources where id <> 0;
      delete from rotten.physical_sources;
      delete from rotten.controllers;
      delete from rotten.actions;
      delete from rotten.job_tags;
    SQL
  end

  def self.seed!
    conn = connect
    conn.transaction do
      clear(conn)
      epoch = conn.exec("select extract(epoch from date_trunc('minute', now()))::bigint").getvalue(0, 0)
      anchor = Time.at(Integer(epoch)).utc

      source_ids = {}
      physical_ids = {}
      SOURCES.each do |s|
        source_ids[s.key] = conn.exec_params(
          "insert into rotten.logical_sources (project, environment, cluster, role) values ($1, $2, $3, $4) returning id",
          [s.project, ENVIRONMENT, s.cluster, s.role]
        ).getvalue(0, 0).to_i
        physical_ids[s.key] = conn.exec_params(
          "insert into rotten.physical_sources (fqdn) values ($1) returning id", ["#{s.key}.db.example"]
        ).getvalue(0, 0).to_i
      end

      fingerprint_ids = FINGERPRINTS.to_h do |f|
        [f.key, conn.exec_params("insert into rotten.fingerprints (fingerprint, normalized) values ($1, $2) returning id",
                                 [f.fingerprint, f.normalized]).getvalue(0, 0).to_i]
      end

      dims = Hash.new { |h, k| h[k] = {} }
      dim = lambda do |table, column, name|
        next nil if name.nil?

        dims[table][name] ||= conn.exec_params("insert into rotten.#{table} (#{column}) values ($1) returning id", [name])
                                  .getvalue(0, 0).to_i
      end

      EVENTS.each do |e|
        start = anchor - e.start_ago
        finish = start + WINDOW
        id = conn.exec_params(<<~SQL, [fingerprint_ids[e.fingerprint], source_ids[e.source], physical_ids[e.source], start.iso8601, finish.iso8601, e.calls, e.time]).getvalue(0, 0)
          insert into rotten.events
            (fingerprint_id, logical_source_id, physical_source_id, observed_window_start, observed_window_end, calls, time)
          values ($1, $2, $3, $4, $5, $6, $7) returning id
        SQL
        e.contexts.each do |c|
          conn.exec_params(<<~SQL, [id, start.iso8601, finish.iso8601, dim.("controllers", "controller", c.controller), dim.("actions", "action", c.action), dim.("job_tags", "job_tag", c.job_tag), c.c])
            insert into rotten.event_context
              (event_id, observed_window_start, observed_window_end, controller_id, action_id, job_tag_id, c)
            values ($1, $2, $3, $4, $5, $6, $7)
          SQL
        end
      end
      # Ingest stores these per context row; replica utilization reads only them.
      conn.exec(<<~SQL)
        update rotten.event_context ec
        set logical_source_id = e.logical_source_id,
            attributed_time = e.time * ec.c::double precision / t.total::double precision
        from rotten.events e,
             (select event_id, observed_window_start, sum(c) as total
              from rotten.event_context
              group by event_id, observed_window_start) t
        where e.id = ec.event_id
          and e.observed_window_start = ec.observed_window_start
          and t.event_id = ec.event_id
          and t.observed_window_start = ec.observed_window_start
      SQL

      STATS.each do |s|
        conn.exec_params(<<~SQL, [fingerprint_ids[s.fingerprint], s.source ? source_ids[s.source] : 0, s.type, s.count, s.mean, s.deviation, s.last])
          insert into rotten.fingerprint_stats (fingerprint_id, logical_source_id, type, count, mean, deviation, last)
          values ($1, $2, $3, $4, $5, $6, $7)
        SQL
      end

      Seeded.new(anchor: anchor, fingerprint_ids: fingerprint_ids)
    end
  ensure
    conn&.close
  end

  CROWD_SIZE = 60
  # The crowd's needles: each matches /needle/i one way, its query text, a
  # controller#action or a job tag, and each is small enough to rank below
  # every crowd row. They're all outliers too, with lower scores than the crowd.
  NEEDLES = {
    "needle_sql" => ["select * from haystack_needle where id = $1", ctx("plain", "show", nil, 2)],
    "needle_controller" => ["select * from plain_table where id = $1", ctx("needles", "show", nil, 2)],
    "needle_job" => ["select * from other_plain_table where id = $1", ctx(nil, nil, "NeedleJob", 2)]
  }.freeze

  # Adds CROWD_SIZE big fingerprints to canvas 13 primary, 30 minutes before
  # anchor, each with its own controller#action and job tag, plus the
  # NEEDLES, so every report's limit fills with the crowd. Also adds "stale",
  # whose only /stalectl/ context is 5 hours old. Returns the needles' and
  # stale's fingerprint ids by key.
  def self.seed_crowd!(anchor)
    conn = connect
    conn.transaction do
      source_id = conn.exec("select id from rotten.logical_sources where project = 'canvas' and cluster = '13' and role = 'primary'")
                      .getvalue(0, 0).to_i
      physical_id = conn.exec("select id from rotten.physical_sources where fqdn = 'canvas13p.db.example'").getvalue(0, 0).to_i
      insert_dim = lambda do |table, column, name|
        next nil if name.nil?

        conn.exec_params("insert into rotten.#{table} (#{column}) values ($1) on conflict (#{column}) do update set #{column} = excluded.#{column} returning id",
                         [name]).getvalue(0, 0).to_i
      end
      add_event = lambda do |fingerprint_id, start_ago, calls, time, contexts|
        start = anchor - start_ago
        finish = start + WINDOW
        event_id = conn.exec_params(<<~SQL, [fingerprint_id, source_id, physical_id, start.iso8601, finish.iso8601, calls, time]).getvalue(0, 0)
          insert into rotten.events
            (fingerprint_id, logical_source_id, physical_source_id, observed_window_start, observed_window_end, calls, time)
          values ($1, $2, $3, $4, $5, $6, $7) returning id
        SQL
        total = contexts.sum(&:c)
        contexts.each do |c|
          conn.exec_params(<<~SQL, [event_id, start.iso8601, finish.iso8601, insert_dim.("controllers", "controller", c.controller), insert_dim.("actions", "action", c.action), insert_dim.("job_tags", "job_tag", c.job_tag), c.c, source_id, time * c.c.to_f / total])
            insert into rotten.event_context
              (event_id, observed_window_start, observed_window_end, controller_id, action_id, job_tag_id, c, logical_source_id, attributed_time)
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
          SQL
        end
      end
      add = lambda do |key, normalized, calls, time, contexts|
        fingerprint_id = conn.exec_params("insert into rotten.fingerprints (fingerprint, normalized) values ($1, $2) returning id",
                                          [key, normalized]).getvalue(0, 0).to_i
        add_event.(fingerprint_id, 30 * M, calls, time, contexts)
        conn.exec_params(<<~SQL, [fingerprint_id, source_id])
          insert into rotten.fingerprint_stats (fingerprint_id, logical_source_id, type, count, mean, deviation, last)
          values ($1, $2, 'mean_time', 100000, 1, 1, 1)
        SQL
        fingerprint_id
      end

      CROWD_SIZE.times do |i|
        calls = 1000 + i
        add.("crowd_#{i}", "select * from crowd_table_#{i} where id = $1", calls, calls * 100,
             [ctx("crowd#{i}", "index", nil, calls / 2), ctx(nil, nil, "CrowdJob#{i}", calls - (calls / 2))])
      end
      # Its only context matching /stalectl/ is from 5 hours before anchor.
      stale = add.("stale", "select * from stale_table where id = $1", 2, 20, [ctx("plain", "show", nil, 2)])
      add_event.(stale, 5 * H, 2, 20, [ctx("stalectl", "index", nil, 2)])
      NEEDLES.to_h { |key, (normalized, context)| [key, add.(key, normalized, 2, 20, [context])] }.merge("stale" => stale)
    end
  ensure
    conn&.close
  end

  # Counts rows in the fixture's tables, to show a request changed nothing.
  def self.table_counts
    conn = connect
    %w[logical_sources physical_sources fingerprints events event_context fingerprint_stats controllers actions job_tags users]
      .to_h { |t| [t, conn.exec("select count(*) from rotten.#{t}").getvalue(0, 0).to_i] }
  ensure
    conn&.close
  end
end
