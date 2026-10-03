require "rails_helper"

RSpec.describe FingerprintsHelper, type: :helper do
  let(:start) { Time.utc(2026, 10, 3, 12, 0) }

  def rows(*values)
    values.each_with_index.map do |value, i|
      { "bucket_start" => start + (i * 600), "bucket_end" => start + ((i + 1) * 600), "calls" => value }
    end
  end

  def chart(series)
    Nokogiri::HTML5.fragment(helper.timeseries_chart(series, value_key: "calls", label: "Calls")).at_css("svg")
  end

  def coordinates(svg)
    svg.css("circle").map { |c| [Float(c["cx"]), Float(c["cy"])] }
  end

  def view_box(svg)
    svg["viewBox"].split.map { |n| Float(n) }
  end

  it "draws one point per bucket with its time and value in data attributes" do
    svg = chart(rows(0, 5, 10))

    expect(svg["role"]).to eq("group")
    expect(svg["class"]).to eq("timeseries-chart")
    expect(svg["data-series"]).to eq("calls")
    expect(svg.at_css("> title").text).to eq("Calls")
    points = svg.css("circle")
    expect(points.map { |p| p["data-time"] }).to eq(%w[2026-10-03T12:00:00Z 2026-10-03T12:10:00Z 2026-10-03T12:20:00Z])
    expect(points.map { |p| p["data-value"] }).to eq(%w[0 5 10])

    xs, ys = coordinates(svg).transpose
    expect(xs).to eq(xs.sort)
    expect(xs.uniq.size).to eq(3)
    expect(ys[0]).to be > ys[1]
    expect(ys[1]).to be > ys[2]
    expect(svg.at_css("polyline")["points"].split.size).to eq(3)
  end

  it "orders points and axis labels by bucket start, whatever order the rows come in" do
    sorted = chart(rows(0, 5, 10))
    shuffled = chart(rows(0, 5, 10).values_at(2, 0, 1))

    expect(shuffled.css("circle").map { |p| p["data-time"] })
      .to eq(%w[2026-10-03T12:00:00Z 2026-10-03T12:10:00Z 2026-10-03T12:20:00Z])
    expect(coordinates(shuffled)).to eq(coordinates(sorted))
    expect(shuffled.at_css("polyline")["points"]).to eq(sorted.at_css("polyline")["points"])
    expect(shuffled.css("text").map(&:text)).to eq(sorted.css("text").map(&:text))
    expect(shuffled.at_css("desc").text).to eq(sorted.at_css("desc").text)
  end

  it "keeps every point inside the drawing" do
    svg = chart(rows(3, 1_000_000, 0.5))
    _, _, width, height = view_box(svg)

    coordinates(svg).each do |x, y|
      expect(x).to be_between(0, width)
      expect(y).to be_between(0, height)
    end
  end

  it "draws an empty series without points and says so" do
    svg = chart([])

    expect(svg["role"]).to eq("group")
    expect(svg.at_css("> title").text).to eq("Calls")
    expect(svg.css("circle")).to be_empty
    expect(svg.css("polyline")).to be_empty
    expect(svg.text).to include("No data")
  end

  it "draws a single point in the middle without dividing by zero" do
    svg = chart(rows(42))
    _, _, width, = view_box(svg)

    points = coordinates(svg)
    expect(points.size).to eq(1)
    x, y = points.first
    expect([x, y]).to all(be_finite)
    expect(x).to be_between(width * 0.25, width * 0.75)
    expect(svg.at_css("circle")["data-value"]).to eq("42")
  end

  it "draws all-zero values on the baseline without dividing by zero" do
    svg = chart(rows(0, 0, 0))

    points = coordinates(svg)
    expect(points.size).to eq(3)
    expect(points.flatten).to all(be_finite)
    expect(points.map(&:last).uniq.size).to eq(1)
  end

  it "draws a single zero point without dividing by zero" do
    points = coordinates(chart(rows(0)))

    expect(points.size).to eq(1)
    expect(points.flatten).to all(be_finite)
  end

  it "uses no style attributes, scripts or event handlers, so the strict CSP allows it" do
    svg = chart(rows(1, 2, 3))

    nodes = [svg, *svg.css("*")]
    expect(nodes.flat_map { |n| n.attributes.keys }.grep(/\A(style|on)/)).to be_empty
    expect(svg.css("script, style")).to be_empty
  end

  it "makes each point focusable, labels it, and gives its bucket end rounded up to the second" do
    series = rows(0, 1234.567)
    series.last["bucket_end"] = start + 1200.25
    svg = chart(series)

    points = svg.css("circle")
    expect(points.map { |p| p["tabindex"] }).to eq(%w[0 -1])
    expect(points.map { |p| p["aria-label"] }).to eq(["Calls: 0 at 2026-10-03 12:00 UTC", "Calls: 1,234.57 at 2026-10-03 12:10 UTC"])
    expect(points.map { |p| p["data-end"] }).to eq(%w[2026-10-03T12:10:00Z 2026-10-03T12:20:01Z])
    expect(points.map { |p| p["data-chart-target"] }.uniq).to eq(["point"])
  end

  it "keeps a whole bucket width between data-time and data-end when bucket starts aren't whole seconds" do
    series = [{ "bucket_start" => start + 0.6, "bucket_end" => start + 600.6, "calls" => 1 }]

    point = chart(series).at_css("circle")
    expect(point["data-time"]).to eq("2026-10-03T12:00:00Z")
    expect(point["data-end"]).to eq("2026-10-03T12:10:00Z")
  end

  it "wraps the chart for the chart controller, with a hidden guide, selection and tooltip" do
    html = Nokogiri::HTML5.fragment(helper.timeseries_chart(rows(1, 2), value_key: "calls", label: "Calls",
                                                                        zoom_url: "/fingerprints/1?reset_range=3h"))
    wrapper = html.at_css("div.chart")

    expect(wrapper["data-controller"]).to eq("chart")
    expect(wrapper["data-chart-zoom-url-value"]).to eq("/fingerprints/1?reset_range=3h")
    expect(wrapper.at_css("svg")["data-chart-label-value"]).to be_nil
    expect(wrapper["data-chart-label-value"]).to eq("Calls")
    expect(wrapper.at_css("svg line.chart-guide.chart-hidden[data-chart-target=guide]")).to be_present
    expect(wrapper.at_css("svg rect.chart-selection.chart-hidden[data-chart-target=selection]")).to be_present
    expect(wrapper.at_css("div.chart-tooltip.chart-hidden[data-chart-target=tooltip]")).to be_present
  end

  it "escapes the label" do
    html = helper.timeseries_chart(rows(1), value_key: "calls", label: "<script>alert(1)</script>")

    expect(html).not_to include("<script>")
    expect(html).to include("&lt;script&gt;")
  end
end
