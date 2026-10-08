require "rails_helper"

RSpec.describe Report do
  it "has one report for each SQL file in the reports directory" do
    files = Dir.children(Rails.configuration.x.reports_dir).grep(/\.sql\z/).sort

    expect(described_class.files.sort).to eq(files)
  end

  it "keeps the fingerprint detail queries out of the report list" do
    internal = %w[fingerprint_contexts fingerprint_sources fingerprint_all_sources]

    expect(described_class.all.map(&:key)).not_to include(*internal)
    internal.each do |key|
      expect(described_class.find(key)).to be_nil
      expect(described_class.internal(key).file).to eq("#{key}.sql")
    end
    expect(described_class.internal("outliers")).to be_nil
  end

  it "loads each report's SQL from the reports directory" do
    described_class.files.each do |file|
      expect(ReportSql.read(file)).to include("$1").and match(/\brotten\.(events|event_context)\b/)
    end
  end

  it "finds a report by key and nothing else" do
    expect(described_class.find("outliers").title).to eq("Outliers")
    expect(described_class.find("../outliers")).to be_nil
    expect(described_class.find("to_s")).to be_nil
    expect(described_class.find(nil)).to be_nil
  end

  it "knows which columns show contexts" do
    context_columns = (described_class.all + [described_class.internal("fingerprint_contexts")])
                      .to_h { |report| [report.key, report.columns.select(&:context?).map(&:key)] }
    expect(context_columns).to eq(
      "top_by_total_time" => ["context"], "top_by_calls" => ["context"], "outliers" => ["context"],
      "replica_utilization_by_controller_action" => ["controller_action"],
      "replica_utilization_by_job" => ["job_tag"], "fingerprint_timeseries" => [],
      "fingerprint_contexts" => ["context"]
    )
  end

  it "refuses to read a file that isn't a known report" do
    expect { ReportSql.read("../config/database.yml") }.to raise_error(ArgumentError)
  end
end
