# The parameters for one report run, validated against strict whitelists
# before anything reaches the database. The source fields (project,
# environment, cluster and role) each take a list of values: values in one
# field are OR'd, the fields are AND'd, and an empty field means every value.
# Each value must be one some source has; one that no source has under the
# fields before it (stale_values) is ignored rather than failing the run, and
# a field whose values are all ignored matches nothing (sources_empty?). The
# range and bucket come from fixed choices, and sort must be one of the
# report's columns. Match and Exclude are lists of Postgres regexes, each
# checked by Postgres itself (match_compiles?). Values then go to the SQL as
# bound parameters, never into its text.
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
  # Match and Exclude each take at most this many patterns.
  MAX_PATTERNS = 20
  PATTERN_FIELDS = %i[match exclude].freeze
  # The source fields, in the order they narrow each other, as the catalog's
  # columns.
  SOURCE_FIELDS = %i[project environment cluster role].freeze
  MAX_SOURCE_VALUES = 100
  # A context filter is "controller#action#job_tag", each part empty when the
  # context has none, so "##" is the untagged context. Controller and action
  # can't hold a #; the job tag, last, may.
  CONTEXT_FORMAT = /\A([^#]*)#([^#]*)#(.*)\z/m
  # The context filter takes at most this many contexts.
  MAX_CONTEXTS = 20
  # Compiles a match pattern without reading any rows.
  MATCH_CHECK_SQL = "SELECT ''::text ~* $1::text".freeze
  MATCH_ERROR_PREFIX = "invalid regular expression: ".freeze

  # Read whatever the report: the dataset (source, role and time window) and
  # the sort. Each report's own fields (Report#own_fields) are read only for
  # it, so a form that sends every report's fields runs any of them. Reports
  # that don't filter by role keep a valid one, so the form carries it on.
  # Match and Exclude are read for every report so they carry across them,
  # but only the reports that filter on them (Report#matches?) use them.
  # So do the context filter and Ignore untagged context; outliers ignores both.
  COMMON_FIELDS = %i[project environment cluster role range from to match exclude context ignore_untagged
                     sort dir sort_report].freeze

  FIELDS = {
    project: "Project", environment: "Environment", cluster: "Cluster", role: "Role", range: "Time range",
    from: "From", to: "To", sort: "Sort", dir: "Direction", primary_role: "Primary role",
    replica_role: "Replica role", fingerprint_id: "Fingerprint ID", bucket: "Bucket", sort_report: "Sort report",
    match: "Match", exclude: "Exclude", context: "Context", ignore_untagged: "Ignore untagged context"
  }.freeze

  attr_reader :report, :catalog, :now, *FIELDS.keys

  validate :values_are_plain_strings
  validate :source_exists
  validate :range_is_valid
  validate :sort_is_a_column
  validate :patterns_are_short
  validate :utilization_roles_are_valid, if: -> { report.utilization? }
  validate :timeseries_is_valid, if: -> { report.timeseries? }
  validate :context_is_valid

  def initialize(report, params, catalog:, now: Time.current)
    @report = report
    @catalog = catalog
    @now = now.utc
    read = COMMON_FIELDS + report.own_fields
    @raw = FIELDS.keys.to_h { |field| [field, read.include?(field) ? params[field] : nil] }
    @raw.each { |field, value| instance_variable_set(:"@#{field}", value.is_a?(String) ? value.strip : nil) }
    # A source field is a list, from repeated parameters (project[]=...) or,
    # from older links, one value. Blanks, as the form's empty one, drop out.
    SOURCE_FIELDS.each { |field| instance_variable_set(:"@#{field}", source_values(@raw[field])) }
    # Spaces can matter in a regex, so patterns are kept as typed. A single
    # match= from older links is one pattern.
    PATTERN_FIELDS.each { |field| instance_variable_set(:"@#{field}", pattern_values(@raw[field])) }
    # A job tag can end in a space, so contexts are kept as typed too.
    @context = pattern_values(@raw[:context])
    @range = DEFAULT_RANGE if @range.blank? && @raw[:range].nil?
    @primary_role = DEFAULT_PRIMARY_ROLE if @primary_role.blank?
    @replica_role = DEFAULT_REPLICA_ROLE if @replica_role.blank?
    # The form's sort carries the report it came from; on another report it's dropped.
    @sort = @dir = nil if Report.find(@sort_report) && @sort_report != report.key
  end

  # The form has been sent. Until then the page shows the form only. Links
  # always carry the range; with every source they carry no source field.
  def submitted? = !@raw[:project].nil? || !@raw[:range].nil?

  # The values of a source field that the run ignores, because no source
  # has them under the fields before it, as { cluster: ["7"] }. Only set once
  # the query is valid.
  def stale_values = @stale_values || {}

  # Some field's values are all ignored, so no source matches and every
  # report is empty. Only set once the query is valid.
  def sources_empty? = @sources_empty == true

  # The values of a source field the run filters on, or [] for every value.
  def effective_values(field) = (@effective_values || {}).fetch(field, [])

  # A time series opened without a fingerprint ID at all, as from an old
  # /reports/:id bookmark, waits for one instead of failing. A blank one from the form fails.
  def needs_fingerprint? = report.timeseries? && @raw[:fingerprint_id].nil?

  # From or To came with a preset range, which ignores them.
  def ignored_custom_range? = !custom? && (@raw[:from].present? || @raw[:to].present?)

  # The regexes the report keeps rows by, OR'd: [] for no filter. Only an
  # empty pattern is dropped; one of only spaces filters on them.
  def match_patterns = match || []

  # The regexes the report drops rows by, before match applies.
  def exclude_patterns = exclude || []

  # Match or Exclude on a report that doesn't filter on them, as the time series.
  def ignored_match? = !report.matches? && (match_patterns.any? || exclude_patterns.any?)

  # Asks Postgres to compile each pattern, through the report's runner so it
  # counts against the same timeout. Each pattern it can't compile adds an
  # error naming it, and then this returns false. Reports that ignore match
  # skip the check.
  def match_compiles?(runner)
    return true unless report.matches?

    PATTERN_FIELDS.map { |field| public_send(:"#{field}_patterns").map { |pattern| pattern_compiles?(runner, field, pattern) }.all? }.all?
  end

  def pattern_compiles?(runner, field, pattern)
    runner.run(MATCH_CHECK_SQL, [pattern])
    true
  rescue ActiveRecord::QueryCanceled
    raise
  rescue ActiveRecord::StatementInvalid => e
    raise unless e.cause.is_a?(PG::InvalidRegularExpression) || e.cause.is_a?(PG::ProgramLimitExceeded)

    detail = e.cause.result&.error_field(PG::PG_DIAG_MESSAGE_PRIMARY).to_s.delete_prefix(MATCH_ERROR_PREFIX)
    errors.add(field, :invalid_pattern, message: "pattern #{pattern.inspect} is not a valid regular expression: #{detail}")
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
    own = report == self.report
    # The time series restricted to contexts is another query with the same columns.
    report = Report.internal("fingerprint_context_timeseries") if own && report.timeseries? && context_filtered?
    # Reports that bind the source fields have nothing to read; the
    # all-sources one doesn't read them.
    return [] if sources_empty? && report.kind != :fingerprint_all_sources

    result = runner.run(report.sql, binds(report))
    rows = result.rows.map { |values| with_source(result.columns.zip(values).to_h) }
    own ? sort_rows(rows) : rows
  end

  # The contexts the reports count calls in, as "controller#action#job_tag"
  # strings ([] for every context).
  def contexts = context || []

  # Ignore untagged context is checked: untagged calls are left out.
  def ignore_untagged? = @raw[:ignore_untagged] == "1"

  # The reports count only some contexts' calls.
  def context_filtered? = contexts.any? || ignore_untagged?

  # The context filter on outliers, which scores whole fingerprints and
  # ignores it.
  def ignored_context? = report.kind == :outliers && context_filtered?

  # A context as a contexts row's "controller", "action" and "job_tag" (nil
  # for a part it doesn't have).
  def self.context_parts(context)
    match = CONTEXT_FORMAT.match(context)
    match && %w[controller action job_tag].zip(match.captures.map(&:presence)).to_h
  end

  # The validated parameters, for links that keep the current choices.
  def link_params(**overrides)
    params = { project: project, environment: environment, cluster: cluster, role: role, range: range }
    params.merge!(from: from, to: to) if custom?
    params.merge!(context_params)
    params.merge!(primary_role: primary_role, replica_role: replica_role) if report.utilization?
    params.merge!(fingerprint_id: fingerprint_id, bucket: bucket.presence) if report.timeseries?
    params.merge!(sort: sort, dir: dir) if sort.present?
    with_match(params.merge(overrides).compact_blank)
  end

  # The source fields only, for links to another report.
  def source_params
    params = { project: project, environment: environment, cluster: cluster, role: role, range: range }
    params.merge!(from: from, to: to) if custom?
    with_match(params.compact_blank.merge(context_params))
  end

  private

  # Contexts go in as typed, after compact_blank.
  def context_params
    params = {}
    params[:context] = contexts if contexts.any?
    params[:ignore_untagged] = "1" if ignore_untagged?
    params
  end

  # The patterns go in after compact_blank, which would drop an all-spaces one.
  def with_match(params)
    params = params.merge(match: match_patterns) if match_patterns.any?
    exclude_patterns.any? ? params.merge(exclude: exclude_patterns) : params
  end

  def binds(report)
    start_at, end_at = window.map { |time| time.utc.iso8601(6) }
    source = %i[project environment cluster].map { |field| text_array(effective_values(field)) }
    role_or_nil = text_array(effective_values(:role))
    patterns = [text_array(match_patterns), text_array(exclude_patterns)]
    context_binds = [text_array(contexts), ignore_untagged?]
    case report.kind
    when :top then [*source, start_at, end_at, ROW_LIMIT, role_or_nil, *patterns, *context_binds]
    when :outliers
      [*source, start_at, end_at, ROW_LIMIT, OUTLIER_SIGMA, OUTLIER_MIN_HISTORY, OUTLIER_RATIO, role_or_nil, *patterns]
    when :utilization then [*source, start_at, end_at, primary_role, replica_role, *patterns, *context_binds]
    when :timeseries
      [*source, fingerprint_id_value, start_at, end_at, "#{bucket_seconds} seconds", role_or_nil]
    when :fingerprint_context_timeseries
      [*source, fingerprint_id_value, start_at, end_at, "#{bucket_seconds} seconds", role_or_nil, *context_binds]
    when :fingerprint_contexts
      [*source, fingerprint_id_value, start_at, end_at, CONTEXT_LIMIT, role_or_nil, ignore_untagged?]
    when :fingerprint_sources then [*source, fingerprint_id_value, start_at, end_at, role_or_nil, *context_binds]
    when :fingerprint_all_sources then [fingerprint_id_value, start_at, end_at]
    when :unparsed_summary then [*source, start_at, end_at, role_or_nil, *context_binds]
    else raise ArgumentError, "unknown report kind #{report.kind}"
    end
  end

  # Rows that name their source get it as one "project/environment/cluster"
  # value, for the Source column.
  def with_source(row)
    parts = row.values_at("project", "environment", "cluster")
    parts.all? ? row.merge("source" => parts.join("/")) : row
  end

  # A text[] bind for a list of values, or nil (every value) for none.
  def text_array(values) = values.empty? ? nil : PG::TextEncoder::Array.new.encode(values)

  def pattern_values(value)
    values = value.is_a?(Array) ? value : [value]
    return nil unless values.all? { |item| item.nil? || item.is_a?(String) }

    values.compact.reject(&:empty?).uniq
  end

  def source_values(value)
    values = value.is_a?(Array) ? value : [value]
    return nil unless values.all? { |item| item.nil? || item.is_a?(String) }

    values.compact.map(&:strip).reject(&:empty?).uniq
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

      listed = SOURCE_FIELDS.include?(field) || PATTERN_FIELDS.include?(field) || field == :context
      list = listed && value.is_a?(Array)
      # Pattern counts are checked with their lengths, for a clearer message.
      next if (list ? value : [value]).all? { |item| plain_string?(item) } &&
              (!list || !SOURCE_FIELDS.include?(field) || value.length <= MAX_SOURCE_VALUES)

      errors.add(field, "is invalid")
      instance_variable_set(:"@#{field}", listed ? [] : nil)
    end
  end

  def plain_string?(value)
    value.is_a?(String) && value.length <= MAX_VALUE_LENGTH && value.valid_encoding? && !value.match?(/[[:cntrl:]]/)
  end

  # Every value must be one some source has. Then each field, in order,
  # narrows the sources to its values; a value no source under the earlier
  # fields has is stale, and ignored. A field whose values are all stale
  # filters on nothing.
  def source_exists
    return if errors.any?
    return errors.add(:base, "No source matches that project, environment and cluster") if catalog.empty?

    SOURCE_FIELDS.each_with_index do |field, index|
      unknown = public_send(field) - catalog.map { |row| row[index] }
      # A report that doesn't filter by role doesn't fail on one, but doesn't carry it either.
      next @role -= unknown if field == :role && !report.role_filter?

      unknown.each { |value| errors.add(field, :unknown, message: "#{value} is not a known #{FIELDS.fetch(field).downcase}") }
    end
    return if errors.any?

    rows = catalog
    @sources_empty = false
    @stale_values = {}
    @effective_values = {}
    SOURCE_FIELDS.each_with_index do |field, index|
      # A report that doesn't filter by role carries it without narrowing on it.
      next if field == :role && !report.role_filter?

      present = rows.map { |row| row[index] }
      live, stale = public_send(field).partition { |value| present.include?(value) }
      @stale_values[field] = stale if stale.any?
      @effective_values[field] = live
      # A field with values filters on its live ones; with none live, on nothing.
      rows = rows.select { |row| live.include?(row[index]) } if public_send(field).any?
    end
    @sources_empty = rows.empty?
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

  def patterns_are_short
    PATTERN_FIELDS.each do |field|
      patterns = public_send(:"#{field}_patterns")
      errors.add(field, "has too many patterns (at most #{MAX_PATTERNS} patterns)") if patterns.length > MAX_PATTERNS
      patterns.select { |pattern| pattern.length > MAX_MATCH_LENGTH }.each do |pattern|
        errors.add(field, :too_long, message: "pattern #{pattern.truncate(40).inspect} is too long (at most #{MAX_MATCH_LENGTH} characters)")
      end
    end
  end

  def utilization_roles_are_valid
    known = roles | [DEFAULT_PRIMARY_ROLE, DEFAULT_REPLICA_ROLE]
    errors.add(:primary_role, "is not a known role") unless known.include?(primary_role)
    errors.add(:replica_role, "is not a known role") unless known.include?(replica_role)
    errors.add(:replica_role, "must differ from Primary role") if primary_role == replica_role
  end

  def context_is_valid
    errors.add(:context, "has too many contexts (at most #{MAX_CONTEXTS})") if contexts.length > MAX_CONTEXTS
    errors.add(:context, "must look like controller#action#job_tag") unless contexts.all? { |item| item.match?(CONTEXT_FORMAT) }
    errors.add(:ignore_untagged, "must be 1 or left out") unless @raw[:ignore_untagged].nil? || @raw[:ignore_untagged] == "1"
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
