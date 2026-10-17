require "rails_helper"

# Project, Environment, Cluster and Role each take any number of values
# (project[]=...). Values in one field are OR'd, the fields are AND'd, and an
# empty field means every value.
RSpec.describe "Report source filters", type: :request do
  before do
    User.delete_all
    @fixture = ReportFixture.seed!
    user = User.create!(email: "sources-viewer@example.com", name: "Viewer", role: "viewer", active: true)
    post "/__test/sign_in", params: { user_id: user.id }
  end

  def doc = Nokogiri::HTML(response.body)

  # Calls by fingerprint key, from the report table.
  def calls_by_fingerprint
    keys = @fixture.fingerprint_ids.invert
    doc.css("table.report tbody tr").to_h do |row|
      [keys.fetch(row.at_css("td[data-column=fingerprint_id] a").text.to_i),
       row.at_css("td[data-column=calls]").text.delete(",").to_i]
    end
  end

  def top_by_calls(query)
    get "/reports?report=top_by_calls&range=3h&#{query}"
    expect(response).to have_http_status(:ok), response.body[/report-errors.*?<\/div>/m]
    calls_by_fingerprint
  end

  it "returns the rows of every cluster asked for, and no others" do
    both = top_by_calls("project[]=canvas&environment[]=production&cluster[]=13&cluster[]=7")
    # Recent canvas calls on clusters 13 and 7, primary and replica; bridge's are left out.
    expect(both).to eq("users" => 1300, "courses" => 100, "jobs" => 85, "slow" => 20)

    expect(top_by_calls("project[]=canvas&environment[]=production&cluster[]=13"))
      .to eq("users" => 1100, "courses" => 100, "jobs" => 85)
  end

  it "treats an empty field as every value" do
    # Every project: bridge's 25 users calls and 75 courses calls count too.
    expect(top_by_calls("project[]=&cluster[]=13&cluster[]=7"))
      .to eq("users" => 1325, "courses" => 175, "jobs" => 85, "slow" => 20)
  end

  it "ORs the roles" do
    expect(top_by_calls("project[]=canvas&cluster[]=7&role[]=primary&role[]=replica"))
      .to eq("users" => 200, "slow" => 20)
    expect(top_by_calls("project[]=canvas&cluster[]=7&role[]=replica")).to eq("users" => 140)
  end

  it "still reads a single value, as old links send it" do
    expect(top_by_calls("project=canvas&environment=production&cluster=7"))
      .to eq("users" => 200, "slow" => 20)
  end

  it "ignores a value that doesn't exist under the other filters, and says so" do
    rows = top_by_calls("project[]=bridge&cluster[]=13&cluster[]=7")
    expect(rows).to eq("courses" => 75, "users" => 25)
    expect(doc.at_css(".report-stale").text).to include("Cluster 7")
  end

  it "matches nothing when every value of a field is ignored, and says that's why it's empty" do
    get "/reports?report=top_by_calls&range=3h&project[]=bridge&cluster[]=7"
    expect(response).to have_http_status(:ok)
    expect(doc.css("table.report tbody tr")).to be_empty
    banner = doc.at_css(".report-stale").text
    expect(banner).to include("Cluster 7")
    expect(banner).to include("so the report is empty")
  end

  it "keeps the fingerprint page's all-sources row when the picked sources match nothing" do
    id = @fixture.fingerprint_ids.fetch("users")
    get "/fingerprints/#{id}?range=3h&project[]=bridge&cluster[]=7"
    expect(response).to have_http_status(:ok)
    expect(doc.at_css(".report-stale").text).to include("so the report is empty")
    expect(doc.at_css("tr.all-sources td[data-column=calls]").text).not_to be_empty
  end

  it "doesn't narrow or empty a report that doesn't filter by role on a carried role" do
    get "/reports?report=replica_utilization_by_job&range=3h&project[]=bridge&role[]=replica"
    expect(response).to have_http_status(:ok)
    expect(doc.at_css(".report-stale")).to be_nil
    expect(doc.css("table.report tbody td[data-column=job_tag]").map(&:text)).to include("SyncLearners")
  end

  it "rejects a value no source has" do
    get "/reports?report=top_by_calls&range=3h&project[]=canvas&cluster[]=99"
    expect(response).to have_http_status(:unprocessable_content)
    expect(doc.at_css(".report-errors").text).to include("Cluster 99 is not a known cluster")
  end

  it "names each row's source on the reports that can list several" do
    get "/reports?report=replica_utilization_by_job&range=3h&cluster[]=13"
    sources = doc.css("table.report tbody tr").map do |row|
      [row.at_css("td[data-column=job_tag]").text, row.at_css("td[data-column=source]").text]
    end
    expect(sources).to include(["SyncLearners", "bridge/production/13"], ["SendEmail", "canvas/production/13"])

    get "/reports?report=outliers&range=3h&project[]=canvas"
    expect(doc.css("table.report tbody td[data-column=source]").map(&:text)).to eq(["canvas/production/7"])
  end

  it "carries the values into links as repeated parameters" do
    top_by_calls("project[]=canvas&cluster[]=13&cluster[]=7")
    link = doc.at_css("th[data-column=calls] a")["href"]
    query = Rack::Utils.parse_nested_query(URI(link).query)
    expect(query["cluster"]).to eq(%w[13 7])
    expect(query["project"]).to eq(%w[canvas])
  end
end
