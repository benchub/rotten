require "rails_helper"
require Rails.root.join("lib/rotten_ui/report_timeout")

RSpec.describe RottenUi::ReportTimeout do
  it "defaults to 15 seconds" do
    expect(described_class.fetch!({})).to eq(15_000)
    expect(described_class.fetch!({ "ROTTEN_UI_REPORT_TIMEOUT" => " " })).to eq(15_000)
  end

  it "reads seconds from ROTTEN_UI_REPORT_TIMEOUT" do
    expect(described_class.fetch!({ "ROTTEN_UI_REPORT_TIMEOUT" => "30" })).to eq(30_000)
    expect(described_class.fetch!({ "ROTTEN_UI_REPORT_TIMEOUT" => "2.5" })).to eq(2_500)
  end

  ["0", "-1", "abc", "15s", "1e400", "0.0001", "3000000"].each do |value|
    it "refuses #{value.inspect}" do
      expect { described_class.fetch!({ "ROTTEN_UI_REPORT_TIMEOUT" => value }) }
        .to raise_error(/ROTTEN_UI_REPORT_TIMEOUT/)
    end
  end

  it "is loaded into the app config" do
    expect(Rails.configuration.x.report_timeout_ms).to eq(15_000)
  end
end
