require "rails_helper"

# The landing page after login: rotten's own stats and a heat map of the last
# week, read from rotten.home_stats and rotten.home_heatmap, which
# rotten-server refreshes. No Run a report card: the navbar has Reports.
RSpec.describe "Landing page", type: :system do
  before do
    User.delete_all
    ReportFixture.clear_home_stats!
    user = User.create!(email: "home@example.com", name: "Viewer", role: "viewer", active: true)
    visit "/__test/sign_in?user_id=#{user.id}"
  end

  def owner
    conn = ReportFixture.connect
    yield conn
  ensure
    conn&.close
  end

  it "shows the four stats, the heat map and their age, and no Run a report button" do
    owner do |conn|
      conn.exec(<<~SQL)
        insert into rotten.home_stats (computed_at, as_of, events_last_hour, fingerprints, hosts_last_hour, hosts_last_week)
        values (now() - interval '3 minutes', now() - interval '4 minutes', 7230, 15321, 12, 14);
        insert into rotten.home_heatmap (hour, duration_bucket, fingerprints) values
          (date_trunc('hour', now(), 'UTC') - interval '2 hours', 0, 40),
          (date_trunc('hour', now(), 'UTC') - interval '2 hours', 5, 3),
          (date_trunc('hour', now(), 'UTC') - interval '100 hours', -4, 1);
      SQL
    end
    visit "/"

    expect(page).to have_css("h1", text: "Welcome to Rotten")
    within(".home-stats") do
      expect(page).to have_css(".stat", text: /Ingest rate\s*120.5 events\/min/i)
      expect(page).to have_css(".stat", text: /Fingerprints\s*15,321/i)
      expect(page).to have_css(".stat", text: /Hosts, last hour\s*12/i)
      expect(page).to have_css(".stat", text: /Hosts, last week\s*14/i)
    end
    expect(page).to have_text("Updated 3 minutes ago")
    expect(page).to have_css("svg.heatmap .heat-cell", count: 3)
    expect(page).to have_css("svg.heatmap .heat-cell title", text: /40 fingerprints/, visible: :all)
    expect(page).to have_css("svg.heatmap .heat-cell title", text: /1 fingerprint\b/, visible: :all)
    expect(page).to have_css("svg.heatmap text", text: "1 ms")
    expect(page).to have_css("svg.heatmap text", text: "100 s")
    expect(page).to have_no_link("Run a report")
    expect(page).to have_no_text("Run a report")
  end

  it "says the stats haven't been computed when the server hasn't refreshed them" do
    visit "/"

    expect(page).to have_css("h1", text: "Welcome to Rotten")
    expect(page).to have_text("rotten-server hasn't computed these stats yet")
    expect(page).to have_no_css("svg.heatmap")
    expect(page).to have_no_link("Run a report")
  end

  it "shows what refresh_home_stats computed from the event tables" do
    ReportFixture.seed!
    owner { |conn| conn.exec("select rotten.refresh_home_stats(now())") }
    visit "/"

    within(".home-stats") do
      expect(page).to have_css(".stat", text: /Fingerprints\s*#{ReportFixture::FINGERPRINTS.size}\b/i)
      expect(page).to have_css(".stat", text: /Hosts, last week\s*#{ReportFixture::SOURCES.size}\b/i)
    end
    expect(page).to have_css("svg.heatmap .heat-cell", minimum: 1)
    expect(page).to have_text("Updated less than a minute ago")
  end
end
