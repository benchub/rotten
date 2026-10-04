require "rails_helper"
require Rails.root.join("lib/rotten_ui/session_lifetime")

RSpec.describe RottenUi::SessionLifetime do
  it "defaults to 12 hours" do
    expect(described_class.fetch!({})).to eq(12 * 3600)
    expect(described_class.fetch!({ "ROTTEN_UI_SESSION_LIFETIME_HOURS" => " " })).to eq(12 * 3600)
  end

  it "reads hours from ROTTEN_UI_SESSION_LIFETIME_HOURS, in whole seconds" do
    expect(described_class.fetch!({ "ROTTEN_UI_SESSION_LIFETIME_HOURS" => "8" })).to eq(8 * 3600)
    expect(described_class.fetch!({ "ROTTEN_UI_SESSION_LIFETIME_HOURS" => " 0.5 " })).to eq(1800)
    expect(described_class.fetch!({ "ROTTEN_UI_SESSION_LIFETIME_HOURS" => "8760" })).to eq(8760 * 3600)
  end

  ["0", "-1", "abc", "12h", "1e400", "NaN", "Infinity", "0.0001", "8761"].each do |value|
    it "refuses #{value.inspect}" do
      expect { described_class.fetch!({ "ROTTEN_UI_SESSION_LIFETIME_HOURS" => value }) }
        .to raise_error(/ROTTEN_UI_SESSION_LIFETIME_HOURS must be a number of hours from 0.01 to 8760, got #{Regexp.escape(value.inspect)}/)
    end
  end

  it "is loaded into the app config" do
    expect(Rails.configuration.x.session_lifetime_seconds).to eq(12 * 3600)
  end
end
