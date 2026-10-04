require "rails_helper"

RSpec.describe "Reports", type: :system do
  before do
    User.delete_all
    @fixture = ReportFixture.seed!
    user = User.create!(email: "reports-viewer@example.com", name: "Viewer", role: "viewer", active: true)
    visit "/__test/sign_in?user_id=#{user.id}"
  end

  # The text of the given columns in each body row, in display order.
  def report_rows(*columns)
    within("table.report") do
      all("tbody tr").map do |row|
        columns.map { |column| row.find("td[data-column='#{column}']").text }
      end
    end
  end

  def pick_source(project:, cluster:, role: nil, range: "Last 3 hours")
    select project, from: "Project"
    select "production", from: "Environment"
    select cluster, from: "Cluster"
    select role, from: "Role" if role
    select range, from: "Time range"
  end

  it "lists every report from the home page" do
    visit "/"
    click_link "Reports"

    expect(page).to have_link("Top queries by total time")
    expect(page).to have_link("Top queries by calls")
    expect(page).to have_link("Outliers")
    expect(page).to have_link("Replica utilization by controller and action")
    expect(page).to have_link("Replica utilization by job")
    expect(page).to have_link("Fingerprint time series")
  end

  it "shows the top queries by total time for a source and range" do
    visit "/reports"
    click_link "Top queries by total time"
    pick_source(project: "canvas", cluster: "13")
    click_button "Run report"

    expect(page).to have_css("table.report")
    expect(report_rows("example", "calls", "total_ms", "avg_ms_per_call")).to eq([
      ["update delayed_jobs set locked_by = $1 where id = $2", "85", "5,200.00", "61.18"],
      ["select * from users where id = $1", "1,100", "530.00", "0.48"],
      ["select * from courses where account_id = $1", "100", "300.00", "3.00"]
    ])
    expect(page).to have_text("SendEmail")
  end

  it "shows the top queries by calls and narrows them by role" do
    visit "/reports/top_by_calls"
    pick_source(project: "canvas", cluster: "13")
    click_button "Run report"

    expect(report_rows("example", "calls")).to eq([
      ["select * from users where id = $1", "1,100"],
      ["select * from courses where account_id = $1", "100"],
      ["update delayed_jobs set locked_by = $1 where id = $2", "85"]
    ])

    select "replica", from: "Role"
    click_button "Run report"

    # The old page already shows replica picked and a report table, so wait
    # for the new page before reading rows.
    expect(page).to have_current_path(/role=replica/)
    expect(page).to have_select("Role", selected: "replica")
    expect(report_rows("example", "calls", "total_ms")).to eq([
      ["select * from users where id = $1", "200", "80.00"],
      ["update delayed_jobs set locked_by = $1 where id = $2", "25", "1,200.00"]
    ])
  end

  it "widens the range to include older windows" do
    visit "/reports/top_by_total_time"
    pick_source(project: "canvas", cluster: "13", range: "Last 6 hours")
    click_button "Run report"

    expect(report_rows("example", "calls", "total_ms")).to eq([
      ["select * from courses where account_id = $1", "9,100", "90,300.00"],
      ["update delayed_jobs set locked_by = $1 where id = $2", "85", "5,200.00"],
      ["select * from users where id = $1", "1,900", "930.00"]
    ])
  end

  it "sorts a report by a column header, both ways" do
    visit "/reports/top_by_total_time"
    pick_source(project: "canvas", cluster: "13")
    click_button "Run report"

    within("table.report thead") { click_link "Calls" }
    expect(page).to have_css("th[data-column='calls'][aria-sort='descending']")
    expect(report_rows("calls")).to eq([["1,100"], ["100"], ["85"]])

    within("table.report thead") { click_link "Calls" }
    expect(page).to have_css("th[data-column='calls'][aria-sort='ascending']")
    expect(report_rows("calls")).to eq([["85"], ["100"], ["1,100"]])
    expect(page).to have_select("Cluster", selected: "13")
  end

  it "shows the outliers for a source" do
    visit "/reports/outliers"
    pick_source(project: "canvas", cluster: "7")
    click_button "Run report"

    expect(report_rows("role", "example", "calls", "total_ms", "avg_ms_per_call")).to eq([
      ["primary", "select * from submissions where assignment_id = $1", "20", "800.00", "40.00"]
    ])
  end

  it "shows replica utilization by job" do
    visit "/reports/replica_utilization_by_job"
    pick_source(project: "canvas", cluster: "13")
    click_button "Run report"

    expect(report_rows("job_tag", "primary_calls", "replica_calls", "primary_call_percent", "replica_call_percent")).to eq([
      ["Reindex", "30", "10", "75.00", "25.00"],
      ["SendEmail", "30", "0", "100.00", "0.00"],
      ["ReplicaReport", "0", "15", "0.00", "100.00"]
    ])
  end

  it "shows replica utilization by controller and action" do
    visit "/reports/replica_utilization_by_controller_action"
    pick_source(project: "canvas", cluster: "13")
    click_button "Run report"

    expect(report_rows("controller_action", "primary_calls", "replica_calls")).to eq([
      ["users#show", "600", "0"],
      ["grades#show", "0", "200"],
      ["users#index", "120", "0"],
      ["courses#index", "100", "0"],
      ["courses#show", "80", "0"],
      ["grades#index", "50", "0"],
      ["api#list", "40", "0"],
      ["login#new", "1", "0"]
    ])
  end

  it "follows a fingerprint through its detail page to its time series over a custom range" do
    visit "/reports/top_by_calls"
    pick_source(project: "canvas", cluster: "13")
    click_button "Run report"

    users_id = @fixture.fingerprint_ids.fetch("users")
    within("table.report") { click_link users_id.to_s }
    expect(page).to have_css("h1", text: "Fingerprint #{users_id}")
    click_link "Time series as a table"

    expect(page).to have_css("h1", text: "Fingerprint time series")
    expect(page).to have_field("Fingerprint ID", with: users_id.to_s)
    expect(page).to have_select("Cluster", selected: "13")

    anchor = @fixture.anchor
    select "Custom", from: "Time range"
    find_field("From (UTC)").set(anchor - (100 * 60))
    find_field("To (UTC)").set(anchor - (20 * 60))
    select "10 minutes", from: "Bucket"
    click_button "Run report"

    expect(page).to have_current_path(/range=custom/)
    label = ->(ago) { (anchor - (ago * 60)).utc.strftime("%Y-%m-%d %H:%M") }
    expect(report_rows("bucket_start", "calls", "total_ms")).to eq([
      [label.(100), "0", "0.00"],
      [label.(90), "300", "150.00"],
      [label.(80), "0", "0.00"],
      [label.(70), "0", "0.00"],
      [label.(60), "0", "0.00"],
      [label.(50), "300", "130.00"],
      [label.(40), "0", "0.00"],
      [label.(30), "500", "250.00"]
    ])
  end

  it "shows a friendly validation error for a source that doesn't exist" do
    visit "/reports/top_by_calls?project=canvas&environment=production&cluster=99&range=3h"

    expect(page).to have_text("No source matches that project, environment and cluster")
    expect(page).not_to have_css("table.report")
  end

  it "shows a friendly message when a report times out" do
    original = Rails.configuration.x.report_timeout_ms
    Rails.configuration.x.report_timeout_ms = 100
    slow = "select pg_sleep(2), $1::text, $2::text, $3::text, $4::timestamptz, $5::timestamptz, $6::integer, $7::text"
    allow(ReportSql).to receive(:read).and_call_original
    allow(ReportSql).to receive(:read).with("top_by_calls.sql").and_return(slow)

    visit "/reports/top_by_calls"
    pick_source(project: "canvas", cluster: "13")
    click_button "Run report"

    expect(page).to have_css(".report-errors", text: "This report took longer than 0.1 seconds and was stopped")
    expect(page).not_to have_css("table.report")
  ensure
    Rails.configuration.x.report_timeout_ms = original
  end
end
