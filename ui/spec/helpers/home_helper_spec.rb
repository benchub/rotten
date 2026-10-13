require "rails_helper"

RSpec.describe HomeHelper, type: :helper do
  let(:as_of) { Time.utc(2026, 10, 12, 12, 30) }

  def cell_titles(html)
    Nokogiri::HTML.fragment(html).css("rect.heat-cell title").map(&:text)
  end

  it "draws one column per hour of the week, and drops cells outside it" do
    cells = {
      [Time.utc(2026, 10, 5, 12), 0] => 1,  # the hour holding as_of - 7 days: the first column
      [Time.utc(2026, 10, 12, 12), 0] => 1, # the hour holding as_of: the last column
      [Time.utc(2026, 10, 5, 11), 0] => 9,  # before the week
      [Time.utc(2026, 10, 12, 13), 0] => 9  # after as_of
    }
    doc = Nokogiri::HTML.fragment(helper.home_heatmap(cells, as_of: as_of))
    rects = doc.css("rect.heat-cell")
    expect(rects.size).to eq(2)
    xs = rects.map { |r| r["x"].to_f }.sort
    width = rects.first["width"].to_f
    expect(xs.first).to eq(HomeHelper::HEATMAP_LEFT)
    expect(xs.last + width).to be_within(0.05).of(HomeHelper::HEATMAP_WIDTH - HomeHelper::HEATMAP_RIGHT)
  end

  it "labels each band with its bounds" do
    cells = { [Time.utc(2026, 10, 12, 9), -4] => 1, [Time.utc(2026, 10, 12, 9), -1] => 2,
              [Time.utc(2026, 10, 12, 9), 6] => 3, [Time.utc(2026, 10, 12, 9), 10] => 1_200 }
    expect(cell_titles(helper.home_heatmap(cells, as_of: as_of))).to contain_exactly(
      "2026-10-12 09:00 UTC, under 0.0316 ms: 1 fingerprint",
      "2026-10-12 09:00 UTC, 0.316 ms to 1 ms: 2 fingerprints",
      "2026-10-12 09:00 UTC, 1 s to 3.16 s: 3 fingerprints",
      "2026-10-12 09:00 UTC, 100 s and up: 1,200 fingerprints"
    )
  end

  it "shades on a log scale from 1 to the busiest cell" do
    hour = Time.utc(2026, 10, 12, 9)
    cells = { [hour, 0] => 1, [hour, 1] => 10, [hour, 2] => 100, [hour, 3] => 10_000 }
    doc = Nokogiri::HTML.fragment(helper.home_heatmap(cells, as_of: as_of))
    levels = doc.css("rect.heat-cell").to_h { |r| [r["data-fingerprints"].to_i, r["class"][/heat-(\d)/, 1].to_i] }
    expect(levels).to eq(1 => 1, 10 => 2, 100 => 3, 10_000 => 5)
  end

  it "says so when the week has no events" do
    html = helper.home_heatmap({}, as_of: as_of)
    expect(html).to include("No events in the last week")
    expect(Nokogiri::HTML.fragment(html).css("rect.heat-cell")).to be_empty
  end
end
