module RottenUi
  # The statement timeout for report queries, from ROTTEN_UI_REPORT_TIMEOUT in
  # seconds. Unset or blank means 15 seconds.
  class ReportTimeout
    DEFAULT_SECONDS = 15
    # Postgres caps statement_timeout at INT_MAX milliseconds.
    MAX_MS = 2_147_483_647
    MESSAGE = "ROTTEN_UI_REPORT_TIMEOUT must be a number of seconds from 0.001 to 2147483"

    # Milliseconds, as statement_timeout takes them.
    def self.fetch!(env = ENV)
      value = env["ROTTEN_UI_REPORT_TIMEOUT"].to_s.strip
      return DEFAULT_SECONDS * 1000 if value.empty?

      seconds = Float(value, exception: false)
      raise "#{MESSAGE}, got #{value.inspect}" unless seconds&.finite?

      ms = (seconds * 1000).round
      raise "#{MESSAGE}, got #{value.inspect}" unless ms.positive? && ms <= MAX_MS

      ms
    end
  end
end
