require "rails_helper"

RSpec.describe "Fingerprint detail", type: :system do
  before do
    User.delete_all
    @fixture = ReportFixture.seed!
    user = User.create!(email: "fingerprint-viewer@example.com", name: "Viewer", role: "viewer", active: true)
    visit "/__test/sign_in?user_id=#{user.id}"
  end

  def table_rows(selector)
    within(selector) do
      all("tbody tr").map { |row| row.all("td").map(&:text) }
    end
  end

  def chart_points(name)
    find("svg.timeseries-chart[data-series='#{name}']").all("circle[data-time][data-value]", visible: :all)
  end

  it "follows a fingerprint from a report to its SQL, chart, top contexts and stats for each source" do
    visit "/reports/top_by_calls"
    select "canvas", from: "Project"
    select "production", from: "Environment"
    select "13", from: "Cluster"
    select "Last 3 hours", from: "Time range"
    click_button "Run report"

    users_id = @fixture.fingerprint_ids.fetch("users")
    within("table.report") { click_link users_id.to_s }

    expect(page).to have_css("h1", text: "Fingerprint #{users_id}")
    expect(find("pre.fingerprint-sql").text).to eq("select * from users where id = $1")
    expect(page).to have_select("Cluster", selected: "13")

    # Three hours in one-minute buckets, picked automatically.
    expect(chart_points("calls").size).to eq(180)

    select "10 minutes", from: "Bucket"
    click_button "Show"

    # The old page has charts too, so wait for the new one before reading them.
    expect(page).to have_current_path(/bucket=10m/)
    calls = chart_points("calls")
    expect(calls.size).to eq(18)
    expect(calls.sum { |point| point["data-value"].to_f }).to eq(1100)
    expect(calls.map { |point| point["data-value"].to_f }.max).to eq(500)
    expect(calls.first["data-time"]).to match(/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\z/)
    total = chart_points("total_ms")
    expect(total.size).to eq(18)
    expect(total.sum { |point| point["data-value"].to_f }).to eq(530)
    expect(page).to have_css("svg.timeseries-chart[role='group'] > title", text: "Calls", visible: :all)

    expect(table_rows("table.fingerprint-contexts")).to eq([
      ["users#show", "600"],
      ["grades#show", "200"],
      ["users#index", "120"],
      ["courses#show", "80"],
      ["grades#index", "50"],
      ["api#list", "40"],
      ["login#new", "1"]
    ])

    expect(table_rows("table.fingerprint-sources")).to eq([
      ["primary", "900", "450.00", "0.50", "30,000", "0.50", "0.10"],
      ["replica", "200", "80.00", "0.40", "", "", ""]
    ])
    # Every project, environment, cluster and role over the same 3 hours,
    # with the all-sources history (logical source 0).
    within("table.fingerprint-sources tfoot tr.all-sources") do
      expect(find("th[scope='row']").text).to eq("All sources")
      expect(all("td").map(&:text)).to eq(["1,325", "640.00", "0.48", "50,000", "0.50", "0.20"])
    end

    csp_violations = page.driver.browser.logs.get(:browser).map(&:message).grep(/Content Security Policy/i)
    expect(csp_violations).to be_empty
  end

  it "narrows the chart, contexts and stats to one role" do
    users_id = @fixture.fingerprint_ids.fetch("users")
    visit "/fingerprints/#{users_id}?project=canvas&environment=production&cluster=13&range=3h&bucket=10m"

    select "replica", from: "Role"
    click_button "Show"

    expect(page).to have_current_path(/role=replica/)
    expect(chart_points("calls").sum { |point| point["data-value"].to_f }).to eq(200)
    expect(table_rows("table.fingerprint-contexts")).to eq([["grades#show", "200"]])
    expect(table_rows("table.fingerprint-sources")).to eq([["replica", "200", "80.00", "0.40", "", "", ""]])
    expect(find("table.fingerprint-sources tfoot tr.all-sources").all("th, td").map(&:text))
      .to eq(["All sources", "1,325", "640.00", "0.48", "50,000", "0.50", "0.20"])
  end

  it "shows the all-sources row when the picked source has no stats" do
    slow_id = @fixture.fingerprint_ids.fetch("slow")
    visit "/fingerprints/#{slow_id}?project=bridge&environment=production&cluster=13&range=3h&bucket=10m"

    expect(page).to have_text("No stats for this source and time range.")
    expect(find("table.fingerprint-sources tfoot tr.all-sources").all("th, td").map(&:text))
      .to eq(["All sources", "20", "800.00", "40.00", "1,001", "5.03", "1.49"])
  end

  it "links to the time series report as a table" do
    users_id = @fixture.fingerprint_ids.fetch("users")
    visit "/fingerprints/#{users_id}?project=canvas&environment=production&cluster=13&range=3h&bucket=10m"

    click_link "Time series as a table"

    expect(page).to have_css("h1", text: "Fingerprint time series")
    expect(page).to have_field("Fingerprint ID", with: users_id.to_s)
    expect(page).to have_css("table.report tbody tr", count: 18)
  end

  describe "chart hover, keyboard and zoom" do
    let(:users_id) { @fixture.fingerprint_ids.fetch("users") }
    let(:base_url) { "/fingerprints/#{users_id}?project=canvas&environment=production&cluster=13&range=3h&bucket=10m" }

    def chart_svg(name) = find("svg.timeseries-chart[data-series='#{name}']")
    def tooltip(name) = chart_svg(name).find(:xpath, "..").find(".chart-tooltip", visible: :all)
    def guide(name) = chart_svg(name).find("line.chart-guide", visible: :all)
    def label_time(iso) = Time.iso8601(iso).utc.strftime("%Y-%m-%d %H:%M")
    def url_query = Rack::Utils.parse_query(URI(page.current_url).query)
    def local(iso) = Time.iso8601(iso).utc.strftime("%Y-%m-%dT%H:%M:%S")

    # Viewport coordinates of the middle of an element.
    def center(element)
      page.evaluate_script(<<~JS, element)
        (function (el) { const r = el.getBoundingClientRect(); return [r.left + r.width / 2, r.top + r.height / 2] })(arguments[0])
      JS
    end

    def drag(from_xy, to_xy)
      page.driver.browser.action
          .move_to_location(from_xy[0].round, from_xy[1].round).pointer_down(:left)
          .move_to_location(((from_xy[0] + to_xy[0]) / 2).round, to_xy[1].round)
          .move_to_location(to_xy[0].round, to_xy[1].round).pointer_up(:left).perform
    end

    def csp_violations = page.driver.browser.logs.get(:browser).map(&:message).grep(/Content Security Policy/i)

    it "shows a guide and a tooltip for the nearest point on hover" do
      visit base_url
      expect(tooltip("calls")).not_to be_visible

      points = chart_points("calls")
      busiest = points.max_by { |point| point["data-value"].to_f }
      busiest.hover

      expect(tooltip("calls")).to have_text("Calls: 500 at #{label_time(busiest['data-time'])} UTC")
      expect(guide("calls")[:class].split).not_to include("chart-hidden")
      expect(guide("calls")[:x1]).to eq(busiest[:cx])

      # Anywhere over the plot picks the point nearest across.
      svg = chart_svg("calls")
      x, y = center(svg)
      nearest = points.min_by { |point| (center(point)[0] - x).abs }
      page.driver.browser.action.move_to_location(x.round, y.round).perform
      expect(tooltip("calls")).to have_text(
        "Calls: #{nearest['data-value'].to_i} at #{label_time(nearest['data-time'])} UTC"
      )

      total = chart_points("total_ms").max_by { |point| point["data-value"].to_f }
      total.hover
      expect(tooltip("total_ms")).to have_text("Total ms: 250 at #{label_time(total['data-time'])} UTC")

      find("h1").hover
      expect(tooltip("total_ms")).not_to be_visible
      expect(csp_violations).to be_empty
    end

    it "shows the tooltip for a point focused from the keyboard" do
      visit base_url
      points = chart_points("calls")

      find_button("Show").send_keys(:tab)
      expect(page.evaluate_script("document.activeElement.dataset.time")).to eq(points[0]["data-time"])
      expect(tooltip("calls")).to have_text("Calls: 0 at #{label_time(points[0]['data-time'])} UTC")
      expect(points[0]["aria-label"]).to eq("Calls: 0 at #{label_time(points[0]['data-time'])} UTC")

      active_time = -> { page.evaluate_script("document.activeElement.dataset.time") }
      active_series = -> { page.evaluate_script("document.activeElement.closest('svg')?.dataset.series ?? null") }
      tab_stops = -> { chart_points("calls").map { |point| point["tabindex"] } }

      # Arrow keys, Home and End move focus and the tooltip within a chart,
      # and the focused point becomes the chart's only Tab stop.
      page.active_element.send_keys(:arrow_right)
      expect(active_time.call).to eq(points[1]["data-time"])
      expect(tooltip("calls")).to have_text("at #{label_time(points[1]['data-time'])} UTC")
      expect(tab_stops.call).to eq(Array.new(points.size) { |i| i == 1 ? "0" : "-1" })

      page.active_element.send_keys(:end)
      expect(active_time.call).to eq(points.last["data-time"])
      expect(tooltip("calls")).to have_text("at #{label_time(points.last['data-time'])} UTC")
      page.active_element.send_keys(:arrow_right)
      expect(active_time.call).to eq(points.last["data-time"])

      page.active_element.send_keys(:home)
      expect(active_time.call).to eq(points[0]["data-time"])
      page.active_element.send_keys(:arrow_left)
      expect(active_time.call).to eq(points[0]["data-time"])
      page.active_element.send_keys(:arrow_right, :arrow_right, :arrow_left)
      expect(active_time.call).to eq(points[1]["data-time"])
      expect(tooltip("calls")).to have_text("Calls: 0 at #{label_time(points[1]['data-time'])} UTC")
      expect(tab_stops.call.count("0")).to eq(1)

      # Tab leaves the chart for the next one, and comes back to the last
      # focused point.
      total = chart_points("total_ms")
      page.active_element.send_keys(:tab)
      expect(active_series.call).to eq("total_ms")
      expect(active_time.call).to eq(total[0]["data-time"])
      expect(tooltip("total_ms")).to have_text("Total ms: 0 at #{label_time(total[0]['data-time'])} UTC")
      expect(tooltip("calls")).not_to be_visible

      page.active_element.send_keys(%i[shift tab])
      expect(active_series.call).to eq("calls")
      expect(active_time.call).to eq(points[1]["data-time"])

      page.active_element.send_keys(:tab)
      page.active_element.send_keys(:tab)
      expect(active_series.call).to be_nil
      expect(page.active_element.text).to eq("Time series as a table")
      expect(csp_violations).to be_empty
    end

    it "zooms to a dragged range, keeps the URL shareable, and resets" do
      visit base_url
      points = chart_points("calls")
      expect(points.size).to eq(18)
      expected_from = local(points[3]["data-time"])
      expected_to = local(points[8]["data-end"])

      # A click is not a drag, so it doesn't zoom.
      points[5].click
      points[6].hover
      expect(tooltip("calls")).to have_text(label_time(points[6]["data-time"]))
      expect(url_query["range"]).to eq("3h")

      drag(center(points[8]), center(points[3]))

      expect(page).to have_current_path(/range=custom/)
      expect(url_query).to include("range" => "custom", "from" => expected_from, "to" => expected_to,
                                   "bucket" => "10m", "reset_range" => "3h", "cluster" => "13")
      zoomed = chart_points("calls")
      expect(zoomed.size).to eq(6)
      expect(local(zoomed.first["data-time"])).to eq(expected_from)
      expect(page).to have_link("Reset zoom")

      # Dragging from left of the first point clamps to it, and a second
      # zoom still resets to the first range.
      svg_left = page.evaluate_script("arguments[0].getBoundingClientRect().left", chart_svg("calls"))
      first_from = local(zoomed[0]["data-time"])
      second_to = local(zoomed[2]["data-end"])
      drag([svg_left + 3, center(zoomed[2])[1]], center(zoomed[2]))
      expect(page).to have_current_path(/to=#{Regexp.escape(CGI.escape(second_to))}/)
      expect(url_query).to include("from" => first_from, "to" => second_to, "reset_range" => "3h")
      expect(chart_points("calls").size).to eq(3)

      page.go_back
      expect(page).to have_current_path(/to=#{Regexp.escape(CGI.escape(expected_to))}/)
      expect(chart_points("calls").size).to eq(6)

      click_link "Reset zoom"
      expect(page).to have_current_path(/[?&]range=3h/)
      expect(url_query.keys).not_to include("from", "to", "reset_range")
      expect(chart_points("calls").size).to eq(18)
      expect(page).to have_no_link("Reset zoom")
      expect(csp_violations).to be_empty
    end
  end

  it "shows the all-sources row as timed out and keeps the rest of the page" do
    users_id = @fixture.fingerprint_ids.fetch("users")
    allow(ReportSql).to receive(:read).and_call_original
    allow(ReportSql).to receive(:read).with("fingerprint_all_sources.sql")
                                      .and_return("select pg_sleep(30), $1::bigint, $2::timestamptz, $3::timestamptz")
    original = Rails.configuration.x.report_timeout_ms
    Rails.configuration.x.report_timeout_ms = 1_000

    visit "/fingerprints/#{users_id}?project=canvas&environment=production&cluster=13&range=3h&bucket=10m"

    expect(chart_points("calls").size).to eq(18)
    expect(table_rows("table.fingerprint-sources")).to eq([
      ["primary", "900", "450.00", "0.50", "30,000", "0.50", "0.10"],
      ["replica", "200", "80.00", "0.40", "", "", ""]
    ])
    row = find("table.fingerprint-sources tfoot tr.all-sources.unavailable")
    expect(row.all("th, td").map(&:text)).to eq(["All sources", "Timed out"])
    expect(find("p.all-sources-note")).to have_text("ran out of the page's report time limit")
    expect(page).to have_no_text("took longer than")
  ensure
    Rails.configuration.x.report_timeout_ms = original
  end

  it "shows the timeout message when the page's queries together run past the timeout" do
    users_id = @fixture.fingerprint_ids.fetch("users")
    original = Rails.configuration.x.report_timeout_ms
    Rails.configuration.x.report_timeout_ms = 300
    allow(ReportSql).to receive(:read).and_wrap_original do |read, file|
      sql = read.call(file).sub(/;\s*\z/, "")
      "with slow as materialized (select pg_sleep(0.25)) select q.* from (\n#{sql}\n) q, slow"
    end

    visit "/fingerprints/#{users_id}?project=canvas&environment=production&cluster=13&range=3h&bucket=10m"

    expect(page).to have_text("This report took longer than 0.3 seconds and was stopped")
    expect(page).to have_no_css("svg.timeseries-chart")
    expect(page).to have_no_css("table.fingerprint-contexts")
  ensure
    Rails.configuration.x.report_timeout_ms = original
  end

  it "shows the SQL and the source picker before a source is picked" do
    jobs_id = @fixture.fingerprint_ids.fetch("jobs")
    visit "/fingerprints/#{jobs_id}"

    expect(find("pre.fingerprint-sql").text).to eq("update delayed_jobs set locked_by = $1 where id = $2")
    expect(page).to have_button("Show")
    expect(page).to have_no_css("svg.timeseries-chart")
  end
end
