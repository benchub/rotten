require "rails_helper"

RSpec.describe "Report match filter", type: :system do
  before do
    User.delete_all
    @fixture = ReportFixture.seed!
    user = User.create!(email: "match-viewer@example.com", name: "Viewer", role: "viewer", active: true)
    visit "/__test/sign_in?user_id=#{user.id}"
  end

  it "filters by a pattern, highlights the matches, and keeps the pattern when switching reports" do
    visit "/reports"
    select "canvas", from: "Project"
    select "production", from: "Environment"
    select "13", from: "Cluster"
    fill_in "Match", with: "Users|#index"
    choose "Top queries by calls"
    click_button "Run report"
    # The new URL shows the new page loaded.
    expect(page).to have_current_path(/[?&]match=Users%7C%23index(&|\z)/)

    within("table.report") do
      expect(all("td[data-column=example]").map(&:text)).to eq([
        "select * from users where id = $1",
        "select * from courses where account_id = $1"
      ])
      expect(first("td[data-column=example] .query-text")).to have_css("mark", text: "users")
      expect(all("td[data-column=context] mark").map(&:text)).to include("users", "#index")
    end

    # Opening the query disclosure keeps the marks.
    find("td[data-column=example] summary", match: :first).click
    expect(page).to have_css("details.query-disclosure[open] .query-text mark", text: "users")

    choose "Replica utilization by controller and action"
    expect(page).to have_field("Match", with: "Users|#index")
    click_button "Run report"
    expect(page).to have_current_path(/[?&]report=replica_utilization_by_controller_action(&|\z)/)

    expect(page).to have_field("Match", with: "Users|#index")
    within("table.report") do
      names = all("td[data-column=controller_action]").map(&:text)
      expect(names).to contain_exactly("users#show", "users#index", "grades#index", "courses#index")
      expect(all("td[data-column=controller_action] mark").map(&:text)).to include("users", "#index")
    end

    choose "Fingerprint time series"
    fill_in "Fingerprint ID", with: @fixture.fingerprint_ids.fetch("users").to_s
    click_button "Run report"
    expect(page).to have_current_path(/[?&]report=fingerprint_timeseries(&|\z)/)
    expect(page).to have_css(".report-ignored-match", text: "Match was ignored")
    expect(page).to have_field("Match", with: "Users|#index")
  end
end
