# The parameters for one report run, validated against strict whitelists
# before anything reaches the database. The source fields must name a
# source that exists, the range and bucket come from fixed choices, and sort
# must be one of the report's columns. Match is a Postgres regex, checked by
# Postgres itself (match_compiles?). Values then go to the SQL as bound
# parameters, never into its text.
class ReportQuery
  include ActiveModel::Validations

  RANGES = {
    "1h" => ["Last hour", 1.hour],
    "3h" => ["Last 3 hours", 3.hours],
    "6h" => ["Last 6 hours", 6.hours],
    "24h" => ["Last 24 hours", 24.hours],
    "7d" => ["Last 7 days", 7.days],
    "custom" => ["Custom", nil]
  }.freeze
  DEFAULT_RANGE = "3h".freeze
  MAX_CUSTOM_SPAN = 31.days

  BUCKETS = {
    "1m" => ["1 minute", 60],
    "5m" => ["5 minutes", 5 * 60],
    "10m" => ["10 minutes", 10 * 60],
    "1h" => ["1 hour", 60 * 60],
    "1d" => ["1 day", 24 * 60 * 60]
  }.freeze
  # With no bucket picked, the smallest one that gives at most this many.
  AUTO_BUCKETS = 200
  MAX_BUCKETS = 10_000

  DIRECTIONS = %w[asc desc].freeze
  DEFAULT_PRIMARY_ROLE = "primary".freeze
  DEFAULT_REPLICA_ROLE = "replica".freeze

  ROW_LIMIT = 50
  CONTEXT_LIMIT = 10
  OUTLIER_SIGMA = 3
  OUTLIER_MIN_HISTORY = 30
  OUTLIER_RATIO = 2

  MAX_FINGERPRINT_ID = (2**63) - 1
  DATETIME = /\A(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?\z/
  MAX_VALUE_LENGTH = 1000
  MAX_MATCH_LENGTH = 200
  # Compiles a match pattern without reading any rows.
  MATCH_CHECK_SQL = "SELECT ''::text ~* $1::text".freeze
  MATCH_ERROR_PREFIX = "invalid regular expression: ".freeze

  # Read whatever the report: the dataset (source, role and time window) and
  # the sort. Each report's own fields (Report#own_fields) are read only for
  # it, so a form that sends every report's fields runs any of them. Reports
  # that don't filter by role keep a valid one, so the form carries it on.
  # Match is read for every report so it carries across them, but only the
  # reports that filter on it (Report#matches?) use it.
  COMMON_FIELDS = %i[project environment cluster role range from to match sort dir sort_report].freeze

  FIELDS = {
    project: "Project", environment: "Environment", cluster: "Cluster", role: "Role", range: "Time range",
    from: "From", to: "To", sort: "Sort", dir: "Direction", primary_role: "Primary role",
    replica_role: "Replica role", fingerprint_id: "Fingerprint ID", bucket: "Bucket", sort_report: "Sort report",
    match: "Match"
  }.freeze

  attr_reader :report, :catalog, :now, *FIELDS.keys

  validate :values_are_plain_strings
  validate :source_exists
  validate :range_is_valid
  validate :sort_is_a_column
  validate :match_is_short
  validate :utilization_roles_are_valid, if: -> { report.utilization? }
  validate :timeseries_is_valid, if: -> { report.timeseries? }

  def initialize(report, params, catalog:, now: Time.current)
    @report = report
    @catalog = catalog
    @now = now.utc
    read = COMMON_FIELDS + report.own_fields
    @raw = FIELDS.keys.to_h { |field| [field, read.include?(field) ? params[field] : nil] }
    @raw.each { |field, value| instance_variable_set(:"@#{field}", value.is_a?(String) ? value.strip : nil) }
    # Spaces can matter in a regex, so match is kept as typed.
    @match = @raw[:match] if @raw[:match].is_a?(String)
    @range = DEFAULT_RANGE if @range.blank? && @raw[:range].nil?
    @primary_role = DEFAULT_PRIMARY_ROLE if @primary_role.blank?
    @replica_role = DEFAULT_REPLICA_ROLE if @replica_role.blank?
    # The form's sort carries the report it came from; on another report it's dropped.
    @sort = @dir = nil if Report.find(@sort_report) && @sort_report != report.key
  end

  # The form has been sent. Until then the page shows the form only.
  def submitted? = !@raw[:project].nil?

  # A time series opened without a fingerprint ID at all, as from an old
  # /reports/:id bookmark, waits for one instead of failing. A blank one from the form fails.
  def needs_fingerprint? = report.timeseries? && @raw[:fingerprint_id].nil?

  # From or To came with a preset range, which ignores them.
  def ignored_custom_range? = !custom? && (@raw[:from].present? || @raw[:to].present?)

  # The regex the report filters on, or nil for no filter. Only an empty
  # match is no filter; one of only spaces filters on them.
  def match_pattern = match.nil? || match.empty? ? nil : match

  # A match on a report that doesn't filter on it, as the time series.
  def ignored_match? = !report.matches? && !match_pattern.nil?

  # Asks Postgres to compile the match pattern, through the report's runner
  # so it counts against the same timeout. A pattern it can't compile adds
  # an error and returns false. Reports that ignore match skip the check.
  def match_compiles?(runner)
    return true unless report.matches? && match_pattern

    runner.run(MATCH_CHECK_SQL, [match_pattern])
    true
  rescue ActiveRecord::QueryCanceled
    raise
  rescue ActiveRecord::StatementInvalid => e
    raise unless e.cause.is_a?(PG::InvalidRegularExpression) || e.cause.is_a?(PG::ProgramLimitExceeded)

    detail = e.cause.result&.error_field(PG::PG_DIAG_MESSAGE_PRIMARY).to_s.delete_prefix(MATCH_ERROR_PREFIX)
    errors.add(:match, "is not a valid regular expression: #{detail}")
    false
  end

  def human_attribute_name(field) = FIELDS.fetch(field)
  def self.human_attribute_name(field, _options = {}) = FIELDS.fetch(field.to_sym) { field.to_s.humanize }

  def projects = catalog.map(&:first).uniq
  def environments = catalog.map { |row| row[1] }.uniq.sort
  def clusters = catalog.map { |row| row[2] }.uniq.sort
  def roles = catalog.map(&:last).uniq.sort

  def custom? = range == "custom"

  def window
    @window ||= if custom?
                  [parse_time(from), parse_time(to)]
                else
                  [@now - RANGES.fetch(range).last, @now]
                end
  end

  def bucket_seconds
    return BUCKETS.fetch(bucket).last if bucket.present?

    span = window.last - window.first
    BUCKETS.values.map(&:last).find { |seconds| span / seconds <= AUTO_BUCKETS } || BUCKETS.values.last.last
  end

  def sort_column = sort.present? ? report.column(sort) : nil
  def direction = dir.presence || (sort_column&.numeric? ? "desc" : "asc")

  # Runs this query's report, or with the same validated parameters one of
  # the internal reports (Report.internal). Only this query's report sorts.
  def run(runner = ReportRunner.new, report: self.report)
    result = runner.run(report.sql, binds(report))
    rows = result.rows.map { |values| result.columns.zip(values).to_h }
    report == self.report ? sort_rows(rows) : rows
  end

  # The validated parameters, for links that keep the current choices.
  def link_params(**overrides)
    params = { project: project, environment: environment, cluster: cluster, range: range }
    params[:role] = role if role.present?
    params.merge!(from: from, to: to) if custom?
    params.merge!(primary_role: primary_role, replica_role: replica_role) if report.utilization?
    params.merge!(fingerprint_id: fingerprint_id, bucket: bucket.presence) if report.timeseries?
    params.merge!(sort: sort, dir: dir) if sort.present?
    with_match(params.merge(overrides).compact_blank)
  end

  # The source fields only, for links to another report.
  def source_params
    params = { project: project, environment: environment, cluster: cluster, range: range }
    params[:role] = role if role.present?
    params.merge!(from: from, to: to) if custom?
    with_match(params.compact_blank)
  end

  private

  # Match goes in after compact_blank, which would drop an all-spaces pattern.
  def with_match(params) = match_pattern ? params.merge(match: match_pattern) : params

  def binds(report)
    start_at, end_at = window.map { |time| time.utc.iso8601(6) }
    source = [project, environment, cluster]
    role_or_nil = role.presence
    case report.kind
    when :top then [*source, start_at, end_at, ROW_LIMIT, role_or_nil, match_pattern]
    when :outliers
      [*source, start_at, end_at, ROW_LIMIT, OUTLIER_SIGMA, OUTLIER_MIN_HISTORY, OUTLIER_RATIO, role_or_nil, match_pattern]
    when :utilization then [*source, start_at, end_at, primary_role, replica_role, match_pattern]
    when :timeseries
      [*source, fingerprint_id_value, start_at, end_at, "#{bucket_seconds} seconds", role_or_nil]
    when :fingerprint_contexts
      [*source, fingerprint_id_value, start_at, end_at, CONTEXT_LIMIT, role_or_nil]
    when :fingerprint_sources then [*source, fingerprint_id_value, start_at, end_at, role_or_nil]
    when :fingerprint_all_sources then [fingerprint_id_value, start_at, end_at]
    else raise ArgumentError, "unknown report kind #{report.kind}"
    end
  end

  # The internal reports share the time series' validated fingerprint id.
  def fingerprint_id_value
    raise ArgumentError, "fingerprint reports run from a time series query" unless self.report.timeseries?

    Integer(fingerprint_id, 10)
  end

  def sort_rows(rows)
    column = sort_column
    return rows unless column

    present, missing = rows.each_with_index.partition { |row, _| !row[column.key].nil? }
    sign = direction == "desc" ? -1 : 1
    present = present.sort do |(a, a_index), (b, b_index)|
      (((sort_key(a[column.key]) <=> sort_key(b[column.key])) || 0) * sign).nonzero? || a_index <=> b_index
    end
    (present + missing).map(&:first)
  end

  def sort_key(value)
    case value
    when String then value.downcase
    when Time then value.to_r
    else value
    end
  end

  def parse_time(value)
    match = DATETIME.match(value.to_s)
    return nil unless match

    parts = match.captures.map { |part| part.to_i }
    time = Time.utc(*parts.first(5), parts[5] || 0)
    return nil unless time.year.between?(1970, 9999) && [time.year, time.month, time.day, time.hour, time.min] == parts.first(5)

    time
  rescue ArgumentError
    nil
  end

  def values_are_plain_strings
    @raw.each do |field, value|
      next if value.nil?

      unless value.is_a?(String) && value.length <= MAX_VALUE_LENGTH && value.valid_encoding? && !value.match?(/[[:cntrl:]]/)
        errors.add(field, "is invalid")
        instance_variable_set(:"@#{field}", nil)
      end
    end
  end

  def source_exists
    return if errors.any?

    matching = catalog.select { |row| row.first(3) == [project, environment, cluster] }
    if matching.empty?
      errors.add(:base, "No source matches that project, environment and cluster")
    elsif role.present? && matching.none? { |row| row.last == role }
      # A report that doesn't filter by role doesn't fail on it, but doesn't carry it either.
      report.role_filter? ? errors.add(:role, "is not a role of that source") : @role = nil
    end
  end

  def range_is_valid
    return errors.add(:range, "is not one of the choices") unless RANGES.key?(range)
    return unless custom?

    from_time, to_time = window
    errors.add(:from, "must be a date and time like 2026-01-31T13:45") unless from_time
    errors.add(:to, "must be a date and time like 2026-01-31T13:45") unless to_time
    return unless from_time && to_time

    if to_time <= from_time
      errors.add(:to, "must be after From")
    elsif to_time - from_time > MAX_CUSTOM_SPAN
      errors.add(:base, "Custom ranges can be at most 31 days")
    end
  end

  def sort_is_a_column
    errors.add(:sort_report, "is not one of the choices") if !sort_report.nil? && !Report.find(sort_report)
    errors.add(:sort, "is not a column of this report") if sort.present? && !sort_column&.sortable?
    errors.add(:dir, "must be asc or desc") if dir.present? && !DIRECTIONS.include?(dir)
  end

  def match_is_short
    errors.add(:match, "is too long (at most #{MAX_MATCH_LENGTH} characters)") if match.to_s.length > MAX_MATCH_LENGTH
  end

  def utilization_roles_are_valid
    known = roles | [DEFAULT_PRIMARY_ROLE, DEFAULT_REPLICA_ROLE]
    errors.add(:primary_role, "is not a known role") unless known.include?(primary_role)
    errors.add(:replica_role, "is not a known role") unless known.include?(replica_role)
    errors.add(:replica_role, "must differ from Primary role") if primary_role == replica_role
  end

  def timeseries_is_valid
    if !fingerprint_id.to_s.match?(/\A[1-9]\d{0,18}\z/) || Integer(fingerprint_id, 10) > MAX_FINGERPRINT_ID
      errors.add(:fingerprint_id, "must be a positive whole number")
    end
    return errors.add(:bucket, "is not one of the choices") if bucket.present? && !BUCKETS.key?(bucket)
    return unless errors.none? { |error| %i[range from to base].include?(error.attribute) }

    count = ((window.last - window.first) / bucket_seconds).ceil
    errors.add(:base, "That range and bucket make too many buckets (at most #{MAX_BUCKETS})") if count > MAX_BUCKETS
  end
end
