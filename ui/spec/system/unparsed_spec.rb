require "rails_helper"

RSpec.describe "Unparsed fingerprints", type: :system do
  before do
    User.delete_all
    @fixture = ReportFixture.seed!
    ReportFixture.mark_unparsed!(@fixture.fingerprint_ids.fetch("users"))
    user = User.create!(email: "unparsed-viewer@example.com", name: "Viewer", role: "viewer", active: true)
    visit "/__test/sign_in?user_id=#{user.id}"
  end

  it "shows how many queries fell back to text fingerprints, badges them, and explains on the fingerprint page" do
    visit "/reports"
    select "canvas", from: "Project"
    select "production", from: "Environment"
    select "13", from: "Cluster"
    select "Last 3 hours", from: "Time range"
    choose "Top queries by calls"
    click_button "Run report"

    expect(page).to have_css(".unparsed-summary",
                             text: "1 query in this window was fingerprinted by its text, with 1,100 calls")
    users_id = @fixture.fingerprint_ids.fetch("users")
    row = find("table.report tbody tr", text: "select * from users where id = $1")
    expect(row).to have_css("td[data-column='fingerprint_id'] .unparsed-badge", text: "unparsed")
    expect(page).to have_css("table.report .unparsed-badge", count: 1)

    within(row) { click_link users_id.to_s }

    expect(page).to have_css("h1", text: "Fingerprint #{users_id}")
    expect(page).to have_css("h1 .unparsed-badge", text: "unparsed")
    expect(page).to have_css(".unparsed-note", text: "fingerprinted by text, the Postgres 17 parser rejected it")
  end
end
