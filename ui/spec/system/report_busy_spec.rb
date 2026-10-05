require "rails_helper"

# While a report runs, the workbench shows a busy indicator and disables
# Run report. The report runner holds each request about 3 seconds. The
# browser can't be queried while its form submission is pending, so a
# watcher installed before the click records what the page shows the moment
# it goes busy, in sessionStorage, which the next page can read.
RSpec.describe "Report busy indicator", type: :system do
  def hold_seconds = 3

  before do
    User.delete_all
    ReportFixture.seed!
    user = User.create!(email: "busy-viewer@example.com", name: "Viewer", role: "viewer", active: true)
    visit "/__test/sign_in?user_id=#{user.id}"
  end

  after { @gate&.close }

  # Holds every report query until 3 seconds after the first one
  # arrives, then runs it, or raises error if given.
  def hold_reports(error: nil)
    @gate = gate = Queue.new
    arrived = Queue.new
    allow_any_instance_of(ReportRunner).to receive(:run).and_wrap_original do |original, *args|
      arrived << true
      gate.pop(timeout: 30)
      raise error if error

      original.call(*args)
    end
    Thread.new do
      arrived.pop(timeout: 30)
      sleep hold_seconds
      gate.close
    end
  end

  def pick_source
    visit "/reports"
    select "canvas", from: "Project"
    select "production", from: "Environment"
    select "13", from: "Cluster"
  end

  # Records the time of the click, and the first state of the page after it
  # with the form busy.
  def watch_for_busy
    page.execute_script(<<~JS)
      sessionStorage.removeItem("busy")
      const form = document.querySelector("form.report-form")
      let clickedAt
      form.addEventListener("click", () => { clickedAt = performance.now() }, { capture: true })
      new MutationObserver((_, observer) => {
        if (form.getAttribute("aria-busy") !== "true") return
        const button = [...form.querySelectorAll("input[type=submit]")].find((b) => b.value === "Run report")
        const spinner = form.querySelector(".spinner")
        const status = document.querySelector("[role=status]")
        sessionStorage.setItem("busy", JSON.stringify({
          ms: performance.now() - clickedAt,
          status: status?.textContent.trim(),
          // Assistive tech may hold back a live region's announcements
          // inside a busy element until it's no longer busy.
          statusInBusy: !!status?.closest("[aria-busy=true]"),
          spinnerVisible: !!spinner?.checkVisibility(),
          buttonDisabled: button.disabled
        }))
        observer.disconnect()
      }).observe(form, { attributes: true, subtree: true, childList: true })
    JS
  end

  def busy_record
    JSON.parse(page.evaluate_script(%(sessionStorage.getItem("busy"))) || "null")
  end

  # Runs the picked report, then checks what the page showed while it ran.
  def run_and_expect_busy
    watch_for_busy
    started = Time.now
    click_button "Run report"
    yield
    expect(Time.now - started).to be >= hold_seconds
    record = busy_record
    expect(record).not_to be_nil, "the form never went busy"
    expect(record["ms"]).to be < 2000
    expect(record).to include("status" => "Running report…", "statusInBusy" => false,
                              "spinnerVisible" => true, "buttonDisabled" => true)
  end

  def expect_idle
    expect(page).to have_no_css("form.report-form[aria-busy]")
    expect(page).to have_no_css(".spinner", visible: :visible)
    expect(page).to have_no_text("Running report…")
    expect(find("[role=status]", visible: :all).text(:all)).to be_empty
    expect(page).to have_button("Run report", disabled: false)
  end

  it "shows a spinner and disables Run report until the results render" do
    hold_reports
    pick_source
    choose "Top queries by calls"
    expect_idle

    run_and_expect_busy { expect(page).to have_css("h2.report-title", text: "Top queries by calls") }
    expect(page).to have_css("table.report")
    expect_idle
  end

  it "shows it for the other reports too" do
    hold_reports
    pick_source
    choose "Outliers"

    run_and_expect_busy { expect(page).to have_css("h2.report-title", text: "Outliers") }
    expect_idle
  end

  it "clears it when the report times out" do
    hold_reports(error: ActiveRecord::QueryCanceled.new("canceling statement due to statement timeout"))
    pick_source
    choose "Top queries by calls"

    run_and_expect_busy { expect(page).to have_text("took longer than") }
    expect_idle
  end

  it "clears it when the page comes back from the back-forward cache" do
    hold_reports
    pick_source
    choose "Top queries by calls"
    run_and_expect_busy { expect(page).to have_css("h2.report-title", text: "Top queries by calls") }

    page.go_back
    expect(page).to have_current_path("/reports")
    expect_idle
  end
end
