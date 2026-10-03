require "rails_helper"

# Normalized SQL and context names come from the observed databases, so
# they're attacker-influenced. The fingerprint page must render them as
# text, never as markup.
RSpec.describe "Fingerprint detail escaping", type: :request do
  let(:hostile_sql) { "select '</code></pre><script>window.pwned = 1</script>' as x" }
  let(:hostile_controller) { "<img src=x onerror=alert(1)>" }

  before do
    User.delete_all
    @fixture = ReportFixture.seed!
    @fingerprint_id = plant_hostile_fingerprint(@fixture.anchor)
    user = User.create!(email: "escaping@example.com", name: "Viewer", role: "viewer", active: true)
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
      conn.exec_params(<<~SQL, [event_id, start.iso8601, finish.iso8601, controller_id])
        insert into rotten.event_context (event_id, observed_window_start, observed_window_end, controller_id, c)
        values ($1, $2, $3, $4, 7)
      SQL
      fingerprint_id.to_i
    end
  ensure
    conn&.close
  end

  it "renders hostile SQL and context names escaped" do
    get "/fingerprints/#{@fingerprint_id}",
        params: { project: "canvas", environment: "production", cluster: "13", range: "3h" }

    expect(response).to have_http_status(:ok)
    expect(response.body).not_to include("<script>window.pwned")
    expect(response.body).not_to include("</code></pre><script>")
    expect(response.body).to include("&lt;script&gt;window.pwned = 1&lt;/script&gt;")
    expect(response.body).not_to include(hostile_controller)
    expect(response.body).to include("&lt;img src=x onerror=alert(1)&gt;")
    expect(response.body).to include("timeseries-chart")
  end
end
