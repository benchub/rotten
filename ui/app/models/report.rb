# The reports the UI can run. Each one is a SQL file in the repo's reports/
# directory, run with bound parameters only. Columns are the ones the page
# shows, in order; every column but context can be sorted.
class Report
  NUMERIC_TYPES = %i[fingerprint count ms percent number].freeze

  Column = Data.define(:key, :label, :type) do
    def sortable? = type != :context
    def numeric? = NUMERIC_TYPES.include?(type)
  end

  attr_reader :key, :title, :description, :file, :kind, :columns

  def initialize(key:, title:, description:, file:, kind:, columns:)
    @key = key
    @title = title
    @description = description
    @file = file
    @kind = kind
    @columns = columns.map { |key, label, type| Column.new(key: key, label: label, type: type) }.freeze
    freeze
  end

  def self.all = LISTED

  def self.find(key) = LISTED.find { |report| report.key == key }

  # The queries the fingerprint detail page runs alongside the time series.
  # They aren't reports of their own, so they're not listed or routable.
  def self.internal(key) = INTERNAL.find { |report| report.key == key }

  # Every SQL file the UI may read.
  def self.files = (LISTED + INTERNAL).map(&:file)

  def column(key) = columns.find { |column| column.key == key }

  # Top and outlier reports filter by one optional role. Utilization compares
  # a primary and a replica role instead.
  def role_filter? = kind != :utilization
  def utilization? = kind == :utilization
  def timeseries? = kind == :timeseries

  def sql = ReportSql.read(file)

  def self.top_columns
    [
      ["fingerprint_id", "Fingerprint", :fingerprint],
      ["example", "Query", :text],
      ["calls", "Calls", :count],
      ["total_ms", "Total ms", :ms],
      ["avg_ms_per_call", "Avg ms/call", :ms],
      ["context", "Top contexts", :context]
    ]
  end

  def self.utilization_columns(name_key, name_label)
    [
      [name_key, name_label, :text],
      ["primary_calls", "Primary calls", :count],
      ["replica_calls", "Replica calls", :count],
      ["total_calls", "Total calls", :count],
      ["primary_call_percent", "Primary calls %", :percent],
      ["replica_call_percent", "Replica calls %", :percent],
      ["primary_total_ms", "Primary ms", :ms],
      ["replica_total_ms", "Replica ms", :ms],
      ["total_ms", "Total ms", :ms],
      ["primary_time_percent", "Primary time %", :percent],
      ["replica_time_percent", "Replica time %", :percent]
    ]
  end

  LISTED = [
    new(key: "top_by_total_time", title: "Top queries by total time", file: "top_by_total_time.sql", kind: :top,
        description: "The queries that spent the most time.", columns: top_columns),
    new(key: "top_by_calls", title: "Top queries by calls", file: "top_by_calls.sql", kind: :top,
        description: "The queries that ran the most.", columns: top_columns),
    new(key: "outliers", title: "Outliers", file: "outliers.sql", kind: :outliers,
        description: "Queries running much slower per call than their history.",
        columns: [
          ["role", "Role", :text],
          ["fingerprint_id", "Fingerprint", :fingerprint],
          ["example", "Query", :text],
          ["calls", "Calls", :count],
          ["total_ms", "Total ms", :ms],
          ["avg_ms_per_call", "Avg ms/call", :ms],
          ["source_mean_ms", "Source mean ms", :ms],
          ["source_deviation_ms", "Source deviation ms", :ms],
          ["global_mean_ms", "Global mean ms", :ms],
          ["deviations_over_source", "Deviations over source", :number],
          ["context", "Top contexts", :context]
        ]),
    new(key: "replica_utilization_by_controller_action", title: "Replica utilization by controller and action",
        file: "replica_utilization_by_controller_action.sql", kind: :utilization,
        description: "How each controller action splits its calls and time between primary and replica.",
        columns: utilization_columns("controller_action", "Controller#action")),
    new(key: "replica_utilization_by_job", title: "Replica utilization by job",
        file: "replica_utilization_by_job.sql", kind: :utilization,
        description: "How each job splits its calls and time between primary and replica.",
        columns: utilization_columns("job_tag", "Job")),
    new(key: "fingerprint_timeseries", title: "Fingerprint time series", file: "fingerprint_timeseries.sql",
        kind: :timeseries, description: "Calls and time for one query over time.",
        columns: [
          ["bucket_start", "Bucket start (UTC)", :time],
          ["bucket_end", "Bucket end (UTC)", :time],
          ["calls", "Calls", :count],
          ["total_ms", "Total ms", :ms]
        ])
  ].freeze

  INTERNAL = [
    new(key: "fingerprint_contexts", title: "Top contexts", file: "fingerprint_contexts.sql",
        kind: :fingerprint_contexts, description: "The contexts that ran one query the most.",
        columns: [
          ["context", "Context", :context],
          ["times", "Calls", :count]
        ]),
    new(key: "fingerprint_sources", title: "Stats for each source", file: "fingerprint_sources.sql",
        kind: :fingerprint_sources, description: "One query's calls, time and history on each role.",
        columns: [
          ["role", "Role", :text],
          ["calls", "Calls", :count],
          ["total_ms", "Total ms", :ms],
          ["avg_ms_per_call", "Avg ms/call", :ms],
          ["history_samples", "History samples", :count],
          ["history_mean_ms", "History mean ms/call", :ms],
          ["history_deviation_ms", "History deviation ms", :ms]
        ])
  ].freeze
end
