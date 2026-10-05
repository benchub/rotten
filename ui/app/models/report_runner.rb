# Runs report SQL with bound parameters, in a read-only transaction with a
# statement timeout and JIT off (JIT compiling costs the reports more than it
# saves; see docs/perf.md). set_config(..., true) is SET LOCAL, so both end
# with the transaction and never reach the next user of the pooled
# connection. A query that runs too long raises ActiveRecord::QueryCanceled.
#
# The timeout is a budget for all the queries one runner runs, starting with
# the first: each query gets only what's left, and once it's spent, run raises
# QueryCanceled without querying. A page that runs several queries through
# one runner is stopped after about one timeout.
class ReportRunner
  Result = Data.define(:columns, :rows)

  attr_reader :timeout_ms

  def initialize(timeout_ms: Rails.configuration.x.report_timeout_ms)
    @timeout_ms = Integer(timeout_ms)
  end

  def run(sql, binds)
    left_ms = remaining_ms
    raise ActiveRecord::QueryCanceled, "report time budget of #{timeout_ms}ms spent" if left_ms <= 0

    ApplicationRecord.with_connection do |conn|
      conn.transaction do
        conn.execute("SET TRANSACTION READ ONLY")
        conn.select_value("SELECT set_config('statement_timeout', $1, true)", "Report timeout", ["#{left_ms}ms"])
        conn.select_value("SELECT set_config('jit', 'off', true)", "Report JIT")
        result = conn.select_all(sql, "Report", binds)
        values = result.cast_values
        values = values.map { |value| [value] } if result.columns.one?
        Result.new(columns: result.columns, rows: values)
      end
    end
  end

  # Milliseconds left of the budget. The clock starts on the first call to
  # this or to run.
  def remaining_ms
    now = Process.clock_gettime(Process::CLOCK_MONOTONIC, :millisecond)
    @deadline_ms ||= now + timeout_ms
    @deadline_ms - now
  end
end
