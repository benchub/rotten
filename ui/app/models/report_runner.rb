# Runs report SQL with bound parameters, in a read-only transaction with a
# statement timeout. set_config(..., true) is SET LOCAL, so the timeout ends
# with the transaction and never reaches the next user of the pooled
# connection. A query that runs too long raises ActiveRecord::QueryCanceled.
class ReportRunner
  Result = Data.define(:columns, :rows)

  attr_reader :timeout_ms

  def initialize(timeout_ms: Rails.configuration.x.report_timeout_ms)
    @timeout_ms = Integer(timeout_ms)
  end

  def run(sql, binds)
    ApplicationRecord.with_connection do |conn|
      conn.transaction do
        conn.execute("SET TRANSACTION READ ONLY")
        conn.select_value("SELECT set_config('statement_timeout', $1, true)", "Report timeout", ["#{timeout_ms}ms"])
        result = conn.select_all(sql, "Report", binds)
        values = result.cast_values
        values = values.map { |value| [value] } if result.columns.one?
        Result.new(columns: result.columns, rows: values)
      end
    end
  end
end
