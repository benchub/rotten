require "rails_helper"

# Clicking a context in a report's context list restricts every report to
# the calls in that context; "Ignore untagged context" leaves out untagged calls.
RSpec.describe "Report context filter", type: :system do
  before do
    User.delete_all
    @fixture = ReportFixture.seed!
    @untagged_only = ReportFixture.add_untagged_fingerprint!("select * from untagged_only", calls: 40, time: 4)
    user = User.create!(email: "context-viewer@example.com", name: "Viewer", role: "viewer", active: true)
    visit "/__test/sign_in?user_id=#{user.id}"
    page.driver.browser.logs.get(:browser)
  end

  def rows(*columns)
    within("table.report") do
      all("tbody tr").map { |row| columns.map { |column| row.find("td[data-column='#{column}']").text } }
    end
  end

  it "filters every report to a clicked context, with counts from that context, and removes it" do
    visit "/reports?report=top_by_calls&project[]=canvas&environment[]=production&cluster[]=13&range=3h"
    expect(rows("example")).to include(["select * from courses where account_id = $1"], ["select * from untagged_only"])

    within("tr", text: "select * from users where id = $1") { click_link "users#show" }
    expect(page).to have_current_path(/context%5B%5D=users%23show%23/)
    # users ran 600 calls in users#show on canvas 13; no other query ran there.
    expect(rows("example", "calls")).to eq([["select * from users where id = $1", "600"]])
    expect(page).to have_css(".report-context-filter", text: "users#show")

    choose "Replica utilization by controller and action"
    click_button "Run report"
    expect(page).to have_css("h2.report-title", text: "Replica utilization by controller and action")
    expect(rows("controller_action", "total_calls")).to eq([["users#show", "600"]])

    click_link "Remove context users#show"
    expect(page).to have_no_css(".report-context-filter")
    expect(rows("controller_action")).to include(["users#index"], ["grades#show"])
    expect(page.driver.browser.logs.get(:browser).map(&:message).grep(/Content Security Policy/i)).to be_empty
  end

  it "adds a second clicked context to the first" do
    visit "/reports?report=top_by_calls&project[]=canvas&environment[]=production&cluster[]=13&range=3h"
    within("tr", text: "select * from users where id = $1") { click_link "users#show" }
    within("tr", text: "select * from users where id = $1") { expect(page).to have_no_link("grades#show") }

    visit current_url.sub("context%5B%5D=users%23show%23", "context%5B%5D=users%23show%23&context%5B%5D=%23%23SendEmail")
    expect(rows("example", "calls")).to contain_exactly(["select * from users where id = $1", "600"],
                                                        ["update delayed_jobs set locked_by = $1 where id = $2", "30"])
    expect(all(".report-context-filter li span").map(&:text)).to eq(%w[users#show SendEmail])
  end

  it "drops untagged calls when Ignore untagged context is checked, and says the totals leave them out" do
    visit "/reports"
    select "canvas", from: "Project"
    select "13", from: "Cluster"
    choose "Top queries by calls"
    click_button "Run report"
    expect(rows("example")).to include(["select * from untagged_only"])

    check "Ignore untagged context"
    click_button "Run report"
    expect(page).to have_current_path(/ignore_untagged=1/)
    expect(rows("example")).not_to include(["select * from untagged_only"])
    expect(page).to have_css(".report-untagged-note", text: "leave out untagged calls")
    expect(page).to have_checked_field("Ignore untagged context")
  end
end
