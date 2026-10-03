require "rails_helper"

RSpec.describe Report do
  it "has one report for each SQL file in the reports directory" do
    files = Dir.children(Rails.configuration.x.reports_dir).grep(/\.sql\z/).sort

    expect(described_class.all.map(&:file).sort).to eq(files)
  end

  it "loads each report's SQL from the reports directory" do
    described_class.all.each do |report|
      expect(ReportSql.read(report.file)).to include("$1", "rotten.logical_sources")
    end
  end

  it "finds a report by key and nothing else" do
    expect(described_class.find("outliers").title).to eq("Outliers")
    expect(described_class.find("../outliers")).to be_nil
    expect(described_class.find("to_s")).to be_nil
    expect(described_class.find(nil)).to be_nil
  end

  it "refuses to read a file that isn't a known report" do
    expect { ReportSql.read("../config/database.yml") }.to raise_error(ArgumentError)
  end
end
