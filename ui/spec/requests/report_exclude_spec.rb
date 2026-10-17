require "rails_helper"

# Match takes several patterns (match[]), OR'd, and Exclude takes several
# too (exclude[]). Excludes apply first, then matches filter what's left.
RSpec.describe "Report match and exclude patterns", type: :request do
  let(:source) { "project[]=canvas&environment[]=production&cluster[]=13&range=3h" }

  before do
    User.delete_all
    @fixture = ReportFixture.seed!
    user = User.create!(email: "exclude@example.com", name: "Viewer", role: "viewer", active: true)
    post "/__test/sign_in", params: { user_id: user.id }
  end

  def doc = Nokogiri::HTML5(response.body)

  def fingerprints
    keys = @fixture.fingerprint_ids.invert
    doc.css("table.report tbody td[data-column=fingerprint_id] a").map { |link| keys.fetch(link.text.to_i) }
  end

  def run(report, query)
    get "/reports?report=#{report}&#{source}&#{query}"
    expect(response).to have_http_status(:ok), doc.at_css(".report-errors")&.text
  end

  %w[top_by_calls top_by_total_time].each do |report|
    it "#{report}: ORs the matches, and drops what an exclude matches first" do
      # users, courses and jobs ran on canvas 13; neither pattern matches a context.
      run(report, "match[]=from+users&match[]=delayed_jobs")
      expect(fingerprints).to contain_exactly("users", "jobs")

      run(report, "match[]=from+users&match[]=delayed_jobs&exclude[]=delayed_jobs")
      expect(fingerprints).to eq(%w[users])

      run(report, "exclude[]=delayed_jobs")
      expect(fingerprints).to contain_exactly("users", "courses")
    end
  end

  it "excludes rows of the utilization reports by name" do
    run("replica_utilization_by_job", "match[]=%5ESend&match[]=%5ERe&exclude[]=%5EReindex%24")
    expect(doc.css("table.report tbody td[data-column=job_tag]").map(&:text)).to contain_exactly("SendEmail", "ReplicaReport")
  end

  it "still reads a single match=, as old links send it" do
    run("top_by_calls", "match=delayed_jobs")
    expect(fingerprints).to eq(%w[jobs])
  end

  it "says which pattern isn't a valid regular expression" do
    get "/reports?report=top_by_calls&#{source}&match[]=users&exclude[]=ok&exclude[]=(unclosed"
    expect(response).to have_http_status(:unprocessable_content)
    expect(doc.at_css(".report-errors").text).to include('Exclude pattern "(unclosed" is not a valid regular expression')
    expect(doc.at_css(".report-errors").text).not_to include('"users"')
  end

  it "limits each pattern's length and the number of patterns" do
    get "/reports?report=top_by_calls&#{source}&match[]=#{'a' * (ReportQuery::MAX_MATCH_LENGTH + 1)}"
    expect(response).to have_http_status(:unprocessable_content)
    expect(doc.at_css(".report-errors").text).to include("at most #{ReportQuery::MAX_MATCH_LENGTH} characters")

    too_many = Array.new(ReportQuery::MAX_PATTERNS + 1) { |i| "exclude[]=p#{i}" }.join("&")
    get "/reports?report=top_by_calls&#{source}&#{too_many}"
    expect(response).to have_http_status(:unprocessable_content)
    expect(doc.at_css(".report-errors").text).to include("at most #{ReportQuery::MAX_PATTERNS} patterns")
  end

  it "carries the patterns into links as repeated parameters" do
    run("top_by_calls", "match[]=from+users&match[]=delayed_jobs&exclude[]=delayed_jobs")
    query = Rack::Utils.parse_nested_query(URI(doc.at_css("th[data-column=calls] a")["href"]).query)
    expect(query["match"]).to eq(["from users", "delayed_jobs"])
    expect(query["exclude"]).to eq(["delayed_jobs"])
  end
end
