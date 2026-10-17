require "rails_helper"

# Source filters go to the report SQL as text[] binds. A value with the
# characters that mean something in an array literal (quotes, commas,
# braces, backslashes) or in SQL must match only itself.
RSpec.describe "Report source arrays", type: :request do
  # Each name gets a source with one event of its own fingerprint, whose
  # query text names it.
  let(:names) { [%q(a"b,c{d}\\e' or 'x'='x), "NULL", "null"] }

  before do
    User.delete_all
    ReportFixture.seed!
    conn = ReportFixture.connect
    names.each_with_index do |name, i|
      source = conn.exec_params("insert into rotten.logical_sources (project, environment, cluster, role) values ($1, 'production', '13', 'primary') returning id", [name]).getvalue(0, 0)
      physical = conn.exec_params("insert into rotten.physical_sources (fqdn) values ($1) returning id", ["tricky#{i}.db.example"]).getvalue(0, 0)
      fingerprint = conn.exec_params("insert into rotten.fingerprints (fingerprint, normalized) values ($1, $1) returning id", ["select tricky_#{i}"]).getvalue(0, 0)
      conn.exec_params(<<~SQL, [fingerprint, source, physical])
        insert into rotten.events
          (fingerprint_id, logical_source_id, physical_source_id, observed_window_start, observed_window_end, calls, time)
        values ($1, $2, $3, date_trunc('minute', now()) - interval '30 minutes', date_trunc('minute', now()) - interval '20 minutes', 7, 1)
      SQL
    end
    conn.close
    user = User.create!(email: "arrays-viewer@example.com", name: "Viewer", role: "viewer", active: true)
    post "/__test/sign_in", params: { user_id: user.id }
  end

  it "matches a value with array, SQL or NULL syntax in it only to itself" do
    names.each_with_index do |name, i|
      get "/reports", params: { report: "top_by_calls", range: "3h", project: [name] }

      expect(response).to have_http_status(:ok), name
      doc = Nokogiri::HTML(response.body)
      expect(doc.css(".report-errors")).to be_empty, name
      expect(doc.css("td[data-column=example]").map(&:text)).to eq(["select tricky_#{i}"]), name
    end
  end

  it "rejects a list that isn't all strings, or is too long" do
    get "/reports", params: { report: "top_by_calls", range: "3h", project: { "a" => "canvas" } }
    expect(response).to have_http_status(:unprocessable_content)

    get "/reports", params: { report: "top_by_calls", range: "3h", cluster: Array.new(ReportQuery::MAX_SOURCE_VALUES + 1) { |i| i.to_s } }
    expect(response).to have_http_status(:unprocessable_content)
    expect(response.body).to include("Cluster is invalid")
  end
end
