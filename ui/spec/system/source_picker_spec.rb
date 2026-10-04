require "rails_helper"

# The report form's source dropdowns narrow to the sources that exist as the
# user picks a project, environment and cluster.
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

  it "limits the environments to the picked project's" do
    visit "/reports/top_by_calls"
    expect(page).to have_select("Project", options: %w[bridge canvas])

    select "bridge", from: "Project"
    expect(page).to have_select("Environment", options: %w[beta production])

    select "canvas", from: "Project"
    expect(page).to have_select("Environment", options: %w[production staging])
    expect(csp_violations).to be_empty
  end

  it "limits the clusters to the picked environment's and the roles to the picked cluster's" do
    visit "/reports/top_by_calls"
    select "canvas", from: "Project"
    select "staging", from: "Environment"
    expect(page).to have_select("Cluster", options: %w[13 21])

    select "21", from: "Cluster"
    expect(page).to have_select("Role", options: ["All roles", "primary"])

    select "13", from: "Cluster"
    expect(page).to have_select("Role", options: ["All roles", "replica"])

    select "production", from: "Environment"
    expect(page).to have_select("Cluster", options: %w[13 7])
    select "13", from: "Cluster"
    expect(page).to have_select("Role", options: ["All roles", "primary", "replica"])
  end

  it "resets the choices that no longer exist when an earlier one changes" do
    visit "/reports/top_by_calls"
    select "canvas", from: "Project"
    select "production", from: "Environment"
    select "13", from: "Cluster"
    select "replica", from: "Role"

    select "bridge", from: "Project"
    expect(page).to have_select("Environment", selected: "production", options: %w[beta production])
    expect(page).to have_select("Cluster", selected: "13", options: %w[13])
    expect(page).to have_select("Role", selected: "All roles", options: ["All roles", "primary"])

    select "beta", from: "Environment"
    expect(page).to have_select("Cluster", selected: "5", options: %w[5])

    select "canvas", from: "Project"
    expect(page).to have_select("Environment", selected: "production", options: %w[production staging])
    expect(page).to have_select("Cluster", selected: "13", options: %w[13 7])
  end

  it "keeps a selection from the query string and narrows around it" do
    visit "/reports/top_by_calls?project=canvas&environment=staging&cluster=21&role=primary&range=3h"

    expect(page).to have_select("Project", selected: "canvas")
    expect(page).to have_select("Environment", selected: "staging", options: %w[production staging])
    expect(page).to have_select("Cluster", selected: "21", options: %w[13 21])
    expect(page).to have_select("Role", selected: "primary", options: ["All roles", "primary"])
    expect(page).to have_no_css(".report-errors")
    expect(page).to have_text("No rows for this source and time range.")

    click_button "Run report"
    expect(page).to have_current_path(/cluster=21/)
    expect(page).to have_select("Cluster", selected: "21", options: %w[13 21])
    expect(csp_violations).to be_empty
  end

  it "keeps a cluster from the query string that doesn't exist, so the form matches the error" do
    visit "/reports/top_by_calls?project=canvas&environment=staging&cluster=7&range=3h"

    expect(page).to have_css(".report-errors", text: "No source matches that project, environment and cluster")
    expect(page).to have_select("Environment", selected: "staging", options: %w[production staging])
    expect(page).to have_select("Cluster", selected: "7", options: %w[13 21 7])

    click_button "Run report"
    expect(page).to have_current_path(/cluster=7/)
    expect(page).to have_css(".report-errors", text: "No source matches that project, environment and cluster")

    select "production", from: "Environment"
    expect(page).to have_select("Cluster", selected: "7", options: %w[13 7])
    select "staging", from: "Environment"
    expect(page).to have_select("Cluster", selected: "13", options: %w[13 21])
  end

  it "keeps a role from the query string that the cluster doesn't have, so the query isn't widened" do
    visit "/reports/top_by_calls?project=canvas&environment=staging&cluster=21&role=replica&range=3h"

    expect(page).to have_css(".report-errors", text: "Role is not a role of that source")
    expect(page).to have_select("Role", selected: "replica", options: ["All roles", "primary", "replica"])

    click_button "Run report"
    expect(page).to have_current_path(/role=replica/)
    expect(page).to have_css(".report-errors", text: "Role is not a role of that source")

    select "13", from: "Cluster"
    expect(page).to have_select("Role", selected: "replica", options: ["All roles", "replica"])
    select "21", from: "Cluster"
    expect(page).to have_select("Role", selected: "All roles", options: ["All roles", "primary"])
  end

  it "narrows the fingerprint page's picker too" do
    visit "/fingerprints/#{@fixture.fingerprint_ids.fetch('users')}"

    select "bridge", from: "Project"
    expect(page).to have_select("Environment", options: %w[beta production])
    select "beta", from: "Environment"
    expect(page).to have_select("Cluster", selected: "5", options: %w[5])
  end
end
