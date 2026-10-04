require "rails_helper"

RSpec.describe "Fingerprint detail", type: :request do
  let(:source_params) { { project: "canvas", environment: "production", cluster: "13", range: "3h" } }

  before do
    User.delete_all
    @fixture = ReportFixture.seed!
  end

  def sign_in
    user = User.create!(email: "viewer-fingerprints@example.com", name: "User", role: "viewer", active: true)
    post "/__test/sign_in", params: { user_id: user.id }
  end

  def users_id = @fixture.fingerprint_ids.fetch("users")

  it "requires login" do
    expect(ReportRunner).not_to receive(:new)

    get "/fingerprints/#{users_id}", params: source_params
    expect(response).to redirect_to("/login")

    get "/fingerprints/#{users_id}"
    expect(response).to redirect_to("/login")

    get "/fingerprints/not-a-number"
    expect(response).to redirect_to("/login")
  end

  it "shows a fingerprint to a viewer" do
    sign_in

    get "/fingerprints/#{users_id}", params: source_params

    expect(response).to have_http_status(:ok)
    expect(response.body).to include("select * from users where id = $1")
    expect(response.body).to include("timeseries-chart")
  end

  it "shows the SQL and the form without running anything until a source is picked" do
    sign_in
    expect(ReportRunner).not_to receive(:new)

    get "/fingerprints/#{users_id}"

    expect(response).to have_http_status(:ok)
    expect(response.body).to include("select * from users where id = $1")
    expect(response.body).not_to include("timeseries-chart")
  end

  it "404s for a fingerprint that doesn't exist" do
    sign_in
    expect(ReportRunner).not_to receive(:new)
    expect(Fingerprint).to receive(:lookup).and_call_original

    get "/fingerprints/#{@fixture.fingerprint_ids.values.max + 1000}", params: source_params

    expect(response).to have_http_status(:not_found)
    expect(response.body).to eq("Not found")
  end

  it "404s from the controller for a malformed fingerprint id, never a 500" do
    sign_in
    expect(ReportRunner).not_to receive(:new)
    ids = ["0", "-1", "01", "1.5", "1e3", "abc", " 1", "1 ", "9223372036854775808", "99999999999999999999",
           "1;select 1", "%27", "0x10", "١"]
    expect(Fingerprint).to receive(:lookup).exactly(ids.size).times.and_call_original

    ids.each do |id|
      get "/fingerprints/#{ERB::Util.url_encode(id)}", params: source_params

      expect(response).to have_http_status(:not_found), "#{id.inspect} -> #{response.status}"
      expect(response.body).to eq("Not found"), "#{id.inspect} wasn't rejected by the controller"
    end
  end

  it "keeps the chart in time order whatever sort is asked for" do
    sign_in

    ["", "&sort=calls&dir=desc", "&sort=bucket_start&dir=asc", "&sort=bucket_start&dir=desc"].each do |extra|
      get "/fingerprints/#{users_id}?#{source_params.merge(bucket: '10m').to_query}#{extra}"

      expect(response).to have_http_status(:ok)
      chart = Nokogiri::HTML5(response.body).at_css("svg[data-series=calls]")
      times = chart.css("circle").map { |p| p["data-time"] }
      expect(times.size).to eq(18), extra
      expect(times).to eq(times.sort), extra
      expect(response.body).not_to include("sort="), extra
    end
  end

  describe "chart hover and zoom markup" do
    def chart_doc(params)
      get "/fingerprints/#{users_id}", params: params
      expect(response).to have_http_status(:ok)
      Nokogiri::HTML5(response.body)
    end

    def query_of(url) = Rack::Utils.parse_query(URI(url).query)

    it "makes each point focusable with a label and its bucket end" do
      sign_in
      doc = chart_doc(source_params.merge(bucket: "10m"))

      chart = doc.at_css("[data-controller=chart]")
      expect(chart).to be_present
      points = doc.css("svg[data-series=calls] circle.chart-point")
      expect(points.size).to eq(18)
      # A roving tabindex: one Tab stop per chart, arrow keys for the rest.
      %w[calls total_ms].each do |series|
        tabindexes = doc.css("svg[data-series=#{series}] circle.chart-point").map { |p| p["tabindex"] }
        expect(tabindexes.first).to eq("0"), series
        expect(tabindexes.drop(1).uniq).to eq(["-1"]), series
      end
      busiest = points.max_by { |p| p["data-value"].to_f }
      time = Time.iso8601(busiest["data-time"]).utc
      expect(busiest["aria-label"]).to eq("Calls: 500 at #{time.strftime('%Y-%m-%d %H:%M')} UTC")
      ends = points.map { |p| Time.iso8601(p["data-end"]) }
      expect(ends.first).to be > Time.iso8601(points.first["data-time"])
      expect(ends.each_cons(2).all? { |a, b| a < b }).to be(true)
    end

    it "carries the pre-zoom range in the zoom URL, and offers no reset link before zooming" do
      sign_in
      doc = chart_doc(source_params.merge(bucket: "10m", role: "primary"))

      zoom = query_of(doc.at_css("[data-controller=chart]")["data-chart-zoom-url-value"])
      expect(zoom).to include("project" => "canvas", "environment" => "production", "cluster" => "13",
                              "role" => "primary", "bucket" => "10m", "reset_range" => "3h")
      expect(zoom.keys).not_to include("range", "from", "to", "reset_from", "reset_to")
      expect(doc.at_css("a.chart-reset")).to be_nil
    end

    it "keeps the first pre-zoom range across zooms and links back to it" do
      sign_in
      now = Time.now.utc
      from = (now - 2.hours).strftime("%Y-%m-%dT%H:%M:%S")
      to = (now - 1.hour).strftime("%Y-%m-%dT%H:%M:%S")
      reset_from = (now - 5.hours).strftime("%Y-%m-%dT%H:%M")
      reset_to = now.strftime("%Y-%m-%dT%H:%M")
      doc = chart_doc(source_params.merge(range: "custom", from: from, to: to, reset_range: "custom",
                                          reset_from: reset_from, reset_to: reset_to))

      zoom = query_of(doc.at_css("[data-controller=chart]")["data-chart-zoom-url-value"])
      expect(zoom).to include("reset_range" => "custom", "reset_from" => reset_from, "reset_to" => reset_to)
      reset = query_of(doc.at_css("a.chart-reset")["href"])
      expect(reset).to eq("project" => "canvas", "environment" => "production", "cluster" => "13",
                          "range" => "custom", "from" => reset_from, "to" => reset_to)
      expect(doc.at_css("a.chart-reset").text).to eq("Reset zoom")
    end

    it "drops from and to from a preset reset range" do
      sign_in
      doc = chart_doc(source_params.merge(range: "custom", from: (Time.now.utc - 2.hours).strftime("%Y-%m-%dT%H:%M"),
                                          to: (Time.now.utc - 1.hour).strftime("%Y-%m-%dT%H:%M"),
                                          reset_range: "24h", reset_from: "junk", reset_to: "junk"))

      expect(query_of(doc.at_css("a.chart-reset")["href"])).to eq(
        "project" => "canvas", "environment" => "production", "cluster" => "13", "range" => "24h"
      )
    end

    it "ignores an invalid reset range rather than linking to it" do
      sign_in
      [{ reset_range: "forever" }, { reset_range: "custom", reset_from: "2026-01-02T00:00", reset_to: "2026-01-01T00:00" },
       { reset_range: "custom", reset_from: "2026-01-01T00:00", reset_to: "2026-03-01T00:00" },
       { reset_range: "custom", reset_from: "<script>" }].each do |extra|
        doc = chart_doc(source_params.merge(extra))

        expect(doc.at_css("a.chart-reset")).to be_nil, extra.inspect
        zoom = query_of(doc.at_css("[data-controller=chart]")["data-chart-zoom-url-value"])
        expect(zoom).to include("reset_range" => "3h"), extra.inspect
        expect(zoom.keys).not_to include("reset_from", "reset_to"), extra.inspect
      end
    end
  end

  it "ignores a fingerprint_id query parameter in favor of the path" do
    sign_in

    get "/fingerprints/#{users_id}", params: source_params.merge(fingerprint_id: @fixture.fingerprint_ids.fetch("jobs"))

    expect(response).to have_http_status(:ok)
    expect(response.body).to include("select * from users where id = $1")
    expect(response.body).to include("users#show")
    expect(response.body).not_to include("SendEmail")
  end

  it "answers 422 with a friendly message for invalid parameters" do
    sign_in
    expect(ReportRunner).not_to receive(:new)

    get "/fingerprints/#{users_id}", params: source_params.merge(range: "forever")

    expect(response).to have_http_status(:unprocessable_content)
    expect(response.body).to include("Time range is not one of the choices")
    expect(response.body).to include("select * from users where id = $1")
  end

  it "answers 422 for too many buckets" do
    sign_in

    get "/fingerprints/#{users_id}", params: source_params.merge(range: "7d", bucket: "1m")

    expect(response).to have_http_status(:unprocessable_content)
    expect(response.body).to include("too many buckets")
  end

  it "answers 503 with a friendly message when a query times out" do
    sign_in
    allow(ReportRunner).to receive(:new).and_wrap_original do |original, **|
      original.call(timeout_ms: 50)
    end
    allow(ReportSql).to receive(:read).and_call_original
    allow(ReportSql).to receive(:read).with("fingerprint_contexts.sql")
                                      .and_return("select pg_sleep(1), $1::text, $2::text, $3::text, $4::bigint, " \
                                                  "$5::timestamptz, $6::timestamptz, $7::integer, $8::text")

    get "/fingerprints/#{users_id}", params: source_params

    expect(response).to have_http_status(:service_unavailable)
    expect(response.body).to include("took longer than")
  end

  it "stops the page within one timeout when every query is slow" do
    sign_in
    get "/fingerprints/#{users_id}", params: source_params
    original_timeout = Rails.configuration.x.report_timeout_ms
    Rails.configuration.x.report_timeout_ms = 300
    allow(ReportSql).to receive(:read).and_wrap_original do |original, file|
      sql = original.call(file).sub(/;\s*\z/, "")
      "with slow as materialized (select pg_sleep(0.25)) select q.* from (\n#{sql}\n) q, slow"
    end

    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    get "/fingerprints/#{users_id}", params: source_params
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

    expect(response).to have_http_status(:service_unavailable)
    expect(response.body).to include("took longer than 0.3\n")
    expect(elapsed).to be < 0.65
  ensure
    Rails.configuration.x.report_timeout_ms = original_timeout if original_timeout
  end

  it "doesn't list the fingerprint detail queries as reports" do
    sign_in

    get "/reports/fingerprint_contexts", params: source_params
    expect(response).to have_http_status(:not_found)

    get "/reports/fingerprint_sources", params: source_params
    expect(response).to have_http_status(:not_found)
  end
end
