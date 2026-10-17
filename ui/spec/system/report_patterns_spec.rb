require "rails_helper"

# Match and Exclude are chip lists of patterns.
RSpec.describe "Report match and exclude patterns", type: :system do
  before do
    User.delete_all
    @fixture = ReportFixture.seed!
    user = User.create!(email: "patterns-viewer@example.com", name: "Viewer", role: "viewer", active: true)
    visit "/__test/sign_in?user_id=#{user.id}"
    page.driver.browser.logs.get(:browser)
  end

  def chips(label) = all("ul[aria-label='#{label} patterns'] li code").map(&:text)

  def examples = all("table.report td[data-column=example]").map(&:text)

  def run_top_by_calls
    choose "Top queries by calls"
    click_button "Run report"
    expect(page).to have_css("h2.report-title", text: "Top queries by calls")
  end

  it "adds match and exclude patterns as chips, filters by them, and removes one" do
    visit "/reports"
    select "canvas", from: "Project"
    select "13", from: "Cluster"
    fill_in "Match", with: "from users"
    find_field("Match").send_keys(:enter)
    fill_in "Match", with: "delayed_jobs"
    click_button "Add match pattern"
    fill_in "Exclude", with: "delayed_jobs"
    find_field("Exclude").send_keys(:enter)
    expect(chips("Match")).to eq(["from users", "delayed_jobs"])
    expect(chips("Exclude")).to eq(["delayed_jobs"])
    expect(page).to have_no_css("h2.report-title")

    run_top_by_calls
    expect(examples).to eq(["select * from users where id = $1"])
    expect(chips("Match")).to eq(["from users", "delayed_jobs"])
    expect(chips("Exclude")).to eq(["delayed_jobs"])

    click_button "Remove exclude pattern delayed_jobs"
    expect(chips("Exclude")).to be_empty
    run_top_by_calls
    expect(examples).to contain_exactly("select * from users where id = $1",
                                        "update delayed_jobs set locked_by = $1 where id = $2")
    expect(page.driver.browser.logs.get(:browser).map(&:message).grep(/Content Security Policy/i)).to be_empty
  end

  it "doesn't add the same pattern twice, and runs a pattern left typed in the field" do
    visit "/reports"
    select "canvas", from: "Project"
    select "13", from: "Cluster"
    2.times do
      fill_in "Match", with: "from users"
      find_field("Match").send_keys(:enter)
    end
    expect(chips("Match")).to eq(["from users"])

    fill_in "Match", with: "delayed_jobs"
    run_top_by_calls
    expect(chips("Match")).to eq(["from users", "delayed_jobs"])
    expect(find_field("Match").value).to eq("")
  end
end
