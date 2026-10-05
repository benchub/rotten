require "rails_helper"

# Fingerprints the worker hashed from the query text, because its Postgres 17
# parser rejected the statement, carry a badge, and the top and outlier
# reports say how many there were in the window.
RSpec.describe "Unparsed fingerprints", type: :request do
  let(:source_params) { { project: "canvas", environment: "production", cluster: "13", range: "3h" } }

  before do
    User.delete_all
    @fixture = ReportFixture.seed!
    user = User.create!(email: "viewer-unparsed@example.com", name: "User", role: "viewer", active: true)
    post "/__test/sign_in", params: { user_id: user.id }
  end

  def doc = Nokogiri::HTML(response.body)
  def id(key) = @fixture.fingerprint_ids.fetch(key)

  def badged_ids
    doc.css("table.report td[data-column=fingerprint_id]").select { |td| td.at_css(".unparsed-badge") }.map { |td| td.at_css("a").text.to_i }
  end

  it "shows no badge or count when nothing fell back" do
    get "/reports", params: source_params.merge(report: "top_by_calls")

    expect(doc.css("table.report tbody tr").size).to eq(3)
    expect(badged_ids).to be_empty
    expect(doc.at_css(".unparsed-summary")).to be_nil
  end

  it "badges unparsed rows in the top reports and counts them for the window" do
    ReportFixture.mark_unparsed!(id("users"))

    %w[top_by_calls top_by_total_time].each do |report|
      get "/reports", params: source_params.merge(report: report)

      expect(response).to have_http_status(:ok)
      expect(badged_ids).to eq([id("users")]), report
      badge = doc.at_css(".unparsed-badge")
      expect(badge.text).to eq("unparsed")
      expect(badge["title"]).to eq("Fingerprinted by text: the Postgres 17 parser rejected it")
      expect(doc.at_css(".unparsed-summary").text.squish)
        .to eq("1 query in this window was fingerprinted by its text, with 1,100 calls: the worker's Postgres 17 parser rejected it.")
    end

    get "/reports", params: source_params.merge(report: "top_by_calls", role: "replica")
    expect(doc.at_css(".unparsed-summary").text).to include("with 200 calls")
  end

  it "counts every unparsed query in the window, not only the listed or matching ones" do
    ReportFixture.mark_unparsed!(id("users"), id("jobs"))

    get "/reports", params: source_params.merge(report: "top_by_calls", match: "account_id")

    expect(doc.css("table.report tbody tr").size).to eq(1)
    expect(badged_ids).to be_empty
    expect(doc.at_css(".unparsed-summary").text.squish)
      .to eq("2 queries in this window were fingerprinted by their text, with 1,185 calls: the worker's Postgres 17 parser rejected them.")
  end

  it "badges unparsed outliers" do
    ReportFixture.mark_unparsed!(id("slow"))

    get "/reports", params: source_params.merge(report: "outliers", cluster: "7")

    expect(badged_ids).to eq([id("slow")])
    expect(doc.at_css(".unparsed-summary").text).to include("1 query in this window")
  end

  it "doesn't count fallbacks on reports without fingerprints" do
    ReportFixture.mark_unparsed!(id("users"))

    get "/reports", params: source_params.merge(report: "replica_utilization_by_job", primary_role: "primary", replica_role: "replica")

    expect(response).to have_http_status(:ok)
    expect(doc.at_css(".unparsed-summary")).to be_nil
  end

  it "badges an unparsed fingerprint's page and says its history won't join a parsed one" do
    ReportFixture.mark_unparsed!(id("users"))

    get "/fingerprints/#{id('users')}"

    expect(doc.at_css("h1 .unparsed-badge").text).to eq("unparsed")
    note = doc.at_css(".unparsed-note").text.squish
    expect(note).to include("fingerprinted by text, the Postgres 17 parser rejected it")
    expect(note).to include("won't join")

    get "/fingerprints/#{id('courses')}"
    expect(doc.at_css(".unparsed-badge")).to be_nil
    expect(doc.at_css(".unparsed-note")).to be_nil
  end
end
