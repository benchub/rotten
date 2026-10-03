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

    calls = chart_points("calls")
    expect(calls.size).to eq(18)
    expect(calls.sum { |point| point["data-value"].to_f }).to eq(1100)
    expect(calls.map { |point| point["data-value"].to_f }.max).to eq(500)
    expect(calls.first["data-time"]).to match(/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\z/)
    total = chart_points("total_ms")
    expect(total.size).to eq(18)
    expect(total.sum { |point| point["data-value"].to_f }).to eq(530)
    expect(page).to have_css("svg.timeseries-chart[role='img'] > title", text: "Calls", visible: :all)

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

    csp_violations = page.driver.browser.logs.get(:browser).map(&:message).grep(/Content Security Policy/i)
    expect(csp_violations).to be_empty
  end

  it "narrows the chart, contexts and stats to one role" do
    users_id = @fixture.fingerprint_ids.fetch("users")
    visit "/fingerprints/#{users_id}?project=canvas&environment=production&cluster=13&range=3h&bucket=10m"

    select "replica", from: "Role"
    click_button "Show"

    expect(chart_points("calls").sum { |point| point["data-value"].to_f }).to eq(200)
    expect(table_rows("table.fingerprint-contexts")).to eq([["grades#show", "200"]])
    expect(table_rows("table.fingerprint-sources")).to eq([["replica", "200", "80.00", "0.40", "", "", ""]])
  end

  it "links to the time series report as a table" do
    users_id = @fixture.fingerprint_ids.fetch("users")
    visit "/fingerprints/#{users_id}?project=canvas&environment=production&cluster=13&range=3h&bucket=10m"

    click_link "Time series as a table"

    expect(page).to have_css("h1", text: "Fingerprint time series")
    expect(page).to have_field("Fingerprint ID", with: users_id.to_s)
    expect(page).to have_css("table.report tbody tr", count: 18)
  end

  it "shows the SQL and the source picker before a source is picked" do
    jobs_id = @fixture.fingerprint_ids.fetch("jobs")
    visit "/fingerprints/#{jobs_id}"

    expect(find("pre.fingerprint-sql").text).to eq("update delayed_jobs set locked_by = $1 where id = $2")
    expect(page).to have_button("Show")
    expect(page).to have_no_css("svg.timeseries-chart")
  end
end
