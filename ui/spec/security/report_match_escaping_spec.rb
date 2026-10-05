require "rails_helper"

# The Match field reaches the database only as a bound parameter, and
# highlighting its matches can't turn query text or context names, which come
# from the observed databases, into markup.
RSpec.describe "Report match escaping", type: :request do
  let(:hostile_sql) { "select '</span></summary><script>window.pwned = 1</script>' as x" }
  let(:hostile_controller) { "<img src=x onerror=alert(1)>" }
  let(:source_params) { { project: "canvas", environment: "production", cluster: "13", range: "3h" } }

  before do
    User.delete_all
    @fixture = ReportFixture.seed!
    plant_hostile_fingerprint(@fixture.anchor)
    user = User.create!(email: "match-escaping@example.com", name: "Viewer", role: "viewer", active: true)
    post "/__test/sign_in", params: { user_id: user.id }
  end

  def plant_hostile_fingerprint(anchor)
    conn = ReportFixture.connect
    conn.transaction do
      source_id = conn.exec("select id from rotten.logical_sources where project = 'canvas' and cluster = '13' and role = 'primary'")
                      .getvalue(0, 0)
      physical_id = conn.exec("select id from rotten.physical_sources where fqdn = 'canvas13p.db.example'").getvalue(0, 0)
      fingerprint_id = conn.exec_params("insert into rotten.fingerprints (fingerprint, normalized) values ($1, $2) returning id",
                                        ["hostile", hostile_sql]).getvalue(0, 0)
      start = anchor - (30 * 60)
      finish = start + ReportFixture::WINDOW
      event_id = conn.exec_params(<<~SQL, [fingerprint_id, source_id, physical_id, start.iso8601, finish.iso8601]).getvalue(0, 0)
        insert into rotten.events
          (fingerprint_id, logical_source_id, physical_source_id, observed_window_start, observed_window_end, calls, time)
        values ($1, $2, $3, $4, $5, 7, 14) returning id
      SQL
      controller_id = conn.exec_params("insert into rotten.controllers (controller) values ($1) returning id", [hostile_controller])
                          .getvalue(0, 0)
      conn.exec_params(<<~SQL, [event_id, start.iso8601, finish.iso8601, controller_id, source_id])
        insert into rotten.event_context (event_id, observed_window_start, observed_window_end, controller_id, c, logical_source_id, attributed_time)
        values ($1, $2, $3, $4, 7, $5, 14)
      SQL
    end
  ensure
    conn&.close
  end

  def doc = Nokogiri::HTML5(response.body)

  it "keeps hostile query text and contexts escaped when the pattern matches the markup" do
    get "/reports", params: source_params.merge(report: "top_by_calls", match: "<script>|</span>|<img src=x")

    expect(response).to have_http_status(:ok)
    expect(response.body).not_to include("<script>window.pwned")
    expect(response.body).not_to include("</span></summary><script>")
    expect(response.body).not_to include(hostile_controller)
    expect(doc.css("table.report script, table.report img")).to be_empty

    example = doc.at_css("td[data-column=example] .query-text")
    expect(example.text).to eq(hostile_sql)
    expect(example["title"]).to eq(hostile_sql)
    expect(example.css("mark").map(&:text)).to eq(["</span>", "<script>"])
    expect(example.css("mark *")).to be_empty
    context = doc.at_css("td[data-column=context] li")
    expect(context.css("mark").map(&:text)).to eq(["<img src=x"])
    expect(response.body).to include("<mark>&lt;img src=x</mark> onerror=alert(1)&gt;#")
  end

  it "matches only the hostile row, so the pattern ran as a regex, not as SQL" do
    get "/reports", params: source_params.merge(report: "top_by_calls", match: "' or '1'='1")
    expect(response).to have_http_status(:ok)
    expect(doc.css("table.report tbody tr")).to be_empty

    get "/reports", params: source_params.merge(report: "top_by_calls", match: "window\\.pwned")
    expect(doc.css("td[data-column=example]").map(&:text)).to eq([hostile_sql])
  end

  it "escapes a hostile pattern in the form and in its error" do
    pattern = "\"><script>window.pwned = 2</script>("
    get "/reports", params: source_params.merge(report: "top_by_calls", match: pattern)

    expect(response).to have_http_status(:unprocessable_content)
    expect(response.body).not_to include("<script>window.pwned = 2")
    expect(doc.at_css("input[name=match]")["value"]).to eq(pattern)
    expect(doc.at_css(".report-errors").text).to include("Match is not a valid regular expression")
  end
end
