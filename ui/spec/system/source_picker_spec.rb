require "rails_helper"

# The report form's source fields: each is a dropdown that adds its values as
# chips. The dropdowns narrow to the sources that exist under the chips of
# the fields before them, and a chip those rule out stays, marked stale.
RSpec.describe "Report source picker", type: :system do
  # Beyond ReportFixture's production sources: canvas/production/{13,7}/
  # {primary,replica} and bridge/production/13/primary.
  let(:extra_sources) do
    [%w[canvas staging 13 replica], %w[canvas staging 21 primary], %w[bridge beta 5 primary]]
  end

  before do
    User.delete_all
    @fixture = ReportFixture.seed!
    conn = ReportFixture.connect
    extra_sources.each do |source|
      conn.exec_params("insert into rotten.logical_sources (project, environment, cluster, role) values ($1, $2, $3, $4)", source)
    end
    conn.close
    user = User.create!(email: "picker-viewer@example.com", name: "Viewer", role: "viewer", active: true)
    visit "/__test/sign_in?user_id=#{user.id}"
    page.driver.browser.logs.get(:browser)
  end

  def csp_violations
    page.driver.browser.logs.get(:browser).map(&:message).grep(/Content Security Policy/i)
  end

  def report_rows(*columns)
    within("table.report") do
      all("tbody tr").map { |row| columns.map { |column| row.find("td[data-column='#{column}']").text } }
    end
  end

  def run_top_by_calls
    choose "Top queries by calls"
    click_button "Run report"
    expect(page).to have_css("h2.report-title", text: "Top queries by calls")
  end

  it "adds picked clusters as chips, removes one, then All empties them, and the rows follow each step" do
    visit "/reports"
    select "canvas", from: "Project"
    select "production", from: "Environment"
    select "13", from: "Cluster"
    select "7", from: "Cluster"
    expect(source_chip_states("Cluster")).to eq([["13", false], ["7", false]])
    expect(page).to have_select("Cluster", with_options: ["All clusters"])
    run_top_by_calls
    expect(page).to have_current_path(/cluster%5B%5D=13&cluster%5B%5D=7/)
    expect(report_rows("example", "calls")).to include(["select * from users where id = $1", "1,300"],
                                                       ["select * from submissions where assignment_id = $1", "20"])

    click_button "Remove cluster 13"
    expect(source_chip_states("Cluster")).to eq([["7", false]])
    run_top_by_calls
    expect(report_rows("example", "calls")).to eq([["select * from users where id = $1", "200"],
                                                   ["select * from submissions where assignment_id = $1", "20"]])

    select "All clusters", from: "Cluster"
    expect(source_chip_states("Cluster")).to be_empty
    run_top_by_calls
    expect(page).to have_no_current_path(/cluster%5B%5D=[^&]/)
    expect(report_rows("example", "calls")).to include(["select * from users where id = $1", "1,300"],
                                                       ["update delayed_jobs set locked_by = $1 where id = $2", "85"])
    expect(csp_violations).to be_empty
  end

  it "doesn't add a value twice, and the chip's remove button works from the keyboard" do
    visit "/reports"
    select "canvas", from: "Project"
    expect(page).to have_no_select("Project", with_options: ["canvas"])
    expect(source_chip_states("Project")).to eq([["canvas", false]])

    find_button("Remove project canvas").send_keys(:enter)
    expect(source_chip_states("Project")).to be_empty
    expect(page).to have_select("Project", selected: "All projects", with_options: %w[bridge canvas])
  end

  it "narrows each dropdown to the sources under the chips before it" do
    visit "/reports"
    expect(page).to have_select("Environment", options: ["All environments", "beta", "production", "staging"])

    select "bridge", from: "Project"
    expect(page).to have_select("Environment", options: ["All environments", "beta", "production"])
    select "canvas", from: "Project"
    expect(page).to have_select("Environment", options: ["All environments", "beta", "production", "staging"])

    click_button "Remove project bridge"
    expect(page).to have_select("Environment", options: ["All environments", "production", "staging"])
    select "staging", from: "Environment"
    expect(page).to have_select("Cluster", options: ["All clusters", "13", "21"])
    select "21", from: "Cluster"
    expect(page).to have_select("Role", with_options: ["All roles", "primary"])
    expect(page).to have_no_select("Role", with_options: ["replica"])
  end

  it "marks a chip the earlier fields rule out as stale, and the server ignores it" do
    visit "/reports"
    select "13", from: "Cluster"
    select "7", from: "Cluster"
    select "bridge", from: "Project"
    expect(source_chip_states("Cluster")).to eq([["13", false], ["7", true]])
    expect(page).to have_css(".source-chip-stale", text: "ignored")

    run_top_by_calls
    expect(page).to have_css(".report-stale", text: "Cluster 7")
    expect(source_chip_states("Cluster")).to eq([["13", false], ["7", true]])
    expect(report_rows("example", "calls")).to eq([["select * from courses where account_id = $1", "75"],
                                                   ["select * from users where id = $1", "25"]])
  end

  it "shows no rows when a field's only chip is ruled out by the other filters" do
    visit "/reports"
    select "canvas", from: "Project"
    select "7", from: "Cluster"
    click_button "Remove project canvas"
    select "bridge", from: "Project"
    expect(source_chip_states("Cluster")).to eq([["7", true]])

    run_top_by_calls
    expect(page).to have_css(".report-stale", text: "so the report is empty")
    expect(page).to have_no_css("table.report tbody tr")
  end

  it "keeps the chips from the query string" do
    visit "/reports?report=top_by_calls&project[]=canvas&environment[]=staging&cluster[]=21&role[]=primary&range=3h"

    expect(source_chip_states("Project")).to eq([["canvas", false]])
    expect(source_chip_states("Environment")).to eq([["staging", false]])
    expect(source_chip_states("Cluster")).to eq([["21", false]])
    expect(source_chip_states("Role")).to eq([["primary", false]])
    expect(page).to have_no_css(".report-errors")
    expect(page).to have_text("No rows for this source and time range.")

    click_button "Run report"
    expect(page).to have_current_path(/cluster%5B%5D=21/)
    expect(source_chip_states("Cluster")).to eq([["21", false]])
    expect(csp_violations).to be_empty
  end

  it "narrows the fingerprint page's picker too" do
    visit "/fingerprints/#{@fixture.fingerprint_ids.fetch('users')}"

    select "bridge", from: "Project"
    expect(page).to have_select("Environment", options: ["All environments", "beta", "production"])
    select "beta", from: "Environment"
    expect(page).to have_select("Cluster", options: ["All clusters", "5"])
  end
end
