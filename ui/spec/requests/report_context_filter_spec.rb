require "rails_helper"

# context[] restricts every report but outliers to the calls in those
# contexts; ignore_untagged=1 leaves out untagged calls.
RSpec.describe "Report context filter", type: :request do
  let(:source) { "project[]=canvas&environment[]=production&cluster[]=13&range=3h" }

  before do
    User.delete_all
    @fixture = ReportFixture.seed!
    user = User.create!(email: "context-filter@example.com", name: "Viewer", role: "viewer", active: true)
    post "/__test/sign_in", params: { user_id: user.id }
  end

  def doc = Nokogiri::HTML5(response.body)

  it "links each listed context to the report with the context added, keeping the rest" do
    get "/reports?report=top_by_calls&#{source}&match[]=users"
    link = doc.css("td[data-column=context] a").find { |a| a.text == "users#show" }
    query = Rack::Utils.parse_nested_query(URI(link["href"]).query)
    expect(query).to include("report" => "top_by_calls", "context" => ["users#show#"], "match" => ["users"], "cluster" => ["13"])
  end

  it "carries the filter into the fingerprint links" do
    get "/reports?report=top_by_calls&#{source}&context[]=users%23show%23&ignore_untagged=1"
    link = doc.at_css("td[data-column=fingerprint_id] a")["href"]
    expect(Rack::Utils.parse_nested_query(URI(link).query)).to include("context" => ["users#show#"], "ignore_untagged" => "1")
  end

  it "says outliers ignores it" do
    get "/reports?report=outliers&project[]=canvas&cluster[]=7&range=3h&context[]=users%23show%23"
    expect(response).to have_http_status(:ok)
    expect(doc.at_css(".report-ignored-context").text).to include("outliers scores whole queries")
    expect(doc.css("table.report tbody tr").size).to eq(1)
  end

  it "rejects an ignore_untagged other than 1" do
    get "/reports?report=top_by_calls&#{source}&ignore_untagged=yes"
    expect(response).to have_http_status(:unprocessable_content)
  end
end
