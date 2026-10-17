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

    ["", "&sort=calls&dir=desc", "&sort=bucket_start&dir=asc", "&sort=bucket_start&dir=desc", "&sort=calls&sort_report=nope"].each do |extra|
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
      expect(zoom).to include("project[]" => "canvas", "environment[]" => "production", "cluster[]" => "13",
                              "role[]" => "primary", "bucket" => "10m", "reset_range" => "3h")
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
      expect(reset).to eq("project[]" => "canvas", "environment[]" => "production", "cluster[]" => "13",
                          "range" => "custom", "from" => reset_from, "to" => reset_to)
      expect(doc.at_css("a.chart-reset").text).to eq("Reset zoom")
    end

    it "drops from and to from a preset reset range" do
      sign_in
      doc = chart_doc(source_params.merge(range: "custom", from: (Time.now.utc - 2.hours).strftime("%Y-%m-%dT%H:%M"),
                                          to: (Time.now.utc - 1.hour).strftime("%Y-%m-%dT%H:%M"),
                                          reset_range: "24h", reset_from: "junk", reset_to: "junk"))

      expect(query_of(doc.at_css("a.chart-reset")["href"])).to eq(
        "project[]" => "canvas", "environment[]" => "production", "cluster[]" => "13", "range" => "24h"
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
                                                  "$5::timestamptz, $6::timestamptz, $7::integer, $8::text, $9::boolean")

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

    # Watch what reaches Postgres instead of timing the page: the timeout
    # each query is given and how many report queries start. Load can only
    # make queries slower, which spends the shared budget sooner, so neither
    # check can fail because the machine is busy.
    timeouts = []
    reports = 0
    watch = lambda do |*, payload|
      case payload[:name]
      when "Report timeout" then timeouts << Integer(Array(payload[:type_casted_binds]).first.to_s.delete_suffix("ms"))
      when "Report" then reports += 1
      end
    end
    ActiveSupport::Notifications.subscribed(watch, "sql.active_record") do
      get "/fingerprints/#{users_id}", params: source_params
    end

    expect(response).to have_http_status(:service_unavailable)
    expect(response.body).to include("took longer than 0.3\n")
    # One timeout per query would give every query 300ms and let all four run.
    # A shared one gives the first 300ms and the next only what the first's
    # 250ms sleep left, so the page stops by the second query.
    expect(timeouts.first).to be <= 300
    expect(timeouts.drop(1)).to all(be <= 50)
    expect(reports).to be_between(1, 2)
  ensure
    Rails.configuration.x.report_timeout_ms = original_timeout if original_timeout
  end

  it "runs the all-sources stats through the page's one runner, after the others" do
    sign_in
    runners = []
    allow(ReportRunner).to receive(:new).and_wrap_original do |original, **kwargs|
      original.call(**kwargs).tap { |runner| runners << runner }
    end
    files = []
    allow(ReportSql).to receive(:read).and_wrap_original do |original, file|
      files << file
      original.call(file)
    end

    get "/fingerprints/#{users_id}", params: source_params

    expect(response).to have_http_status(:ok)
    expect(runners.size).to eq(1)
    expect(files).to eq(%w[fingerprint_timeseries.sql fingerprint_contexts.sql fingerprint_sources.sql
                           fingerprint_all_sources.sql])
  end

  it "keeps the rest of the page when only the all-sources query runs past the timeout" do
    sign_in
    allow(ReportRunner).to receive(:new).and_wrap_original do |original, **|
      original.call(timeout_ms: 1_000)
    end
    allow(ReportSql).to receive(:read).and_call_original
    allow(ReportSql).to receive(:read).with("fingerprint_all_sources.sql")
                                      .and_return("select pg_sleep(2), $1::bigint, $2::timestamptz, $3::timestamptz")

    get "/fingerprints/#{users_id}", params: source_params

    expect(response).to have_http_status(:ok)
    expect(response.body).not_to include("took longer than")
    page = Nokogiri::HTML(response.body)
    expect(page.css("svg.timeseries-chart").size).to eq(2)
    expect(page.css("table.fingerprint-contexts tbody tr").size).to eq(7)
    expect(page.css("table.fingerprint-sources tbody tr").map { |row| row.at_css("td[data-column=role]").text })
      .to eq(%w[primary replica])
    row = page.at_css("table.fingerprint-sources tfoot tr.all-sources.unavailable")
    expect(row.css("th, td").map { |cell| cell.text.strip }).to eq(["All sources", "Timed out"])
    expect(page.at_css("p.all-sources-note").text).to include("ran out of the page's report time limit")
  end

  it "skips the all-sources query when almost none of the budget is left" do
    sign_in
    stub_const("FingerprintsController::ALL_SOURCES_MIN_MS", 10_000_000)
    files = []
    allow(ReportSql).to receive(:read).and_wrap_original do |original, file|
      files << file
      original.call(file)
    end

    get "/fingerprints/#{users_id}", params: source_params

    expect(response).to have_http_status(:ok)
    expect(files).not_to include("fingerprint_all_sources.sql")
    row = Nokogiri::HTML(response.body).at_css("table.fingerprint-sources tfoot tr.all-sources.unavailable")
    expect(row.css("th, td").map { |cell| cell.text.strip }).to eq(["All sources", "Timed out"])
  end

  it "still answers 503 when the per-source query runs past the timeout" do
    sign_in
    allow(ReportRunner).to receive(:new).and_wrap_original do |original, **|
      original.call(timeout_ms: 200)
    end
    allow(ReportSql).to receive(:read).and_call_original
    allow(ReportSql).to receive(:read).with("fingerprint_sources.sql")
                                      .and_return("select pg_sleep(1), $1::text, $2::text, $3::text, $4::bigint, " \
                                                  "$5::timestamptz, $6::timestamptz, $7::text, $8::text, $9::boolean")

    get "/fingerprints/#{users_id}", params: source_params

    expect(response).to have_http_status(:service_unavailable)
    expect(response.body).to include("took longer than")
    expect(response.body).not_to include("all-sources")
  end

  it "doesn't list the fingerprint detail queries as reports" do
    sign_in

    get "/reports/fingerprint_contexts", params: source_params
    expect(response).to have_http_status(:not_found)

    get "/reports/fingerprint_sources", params: source_params
    expect(response).to have_http_status(:not_found)

    get "/reports/fingerprint_all_sources", params: source_params
    expect(response).to have_http_status(:not_found)
  end

  describe "context filter" do
    let(:params) { source_params.merge(bucket: "10m") }

    def doc_for(extra = {})
      get "/fingerprints/#{users_id}", params: params.merge(extra)
      Nokogiri::HTML5(response.body)
    end

    def series_total(doc, name) = doc.css("svg[data-series=#{name}] circle.chart-point").sum { |p| p["data-value"].to_f }
    def query_of(url) = Rack::Utils.parse_query(URI(url).query)

    before { sign_in }

    it "links each context to the page with a context filter, keeping the rest of the choices" do
      doc = doc_for(role: "primary")

      links = doc.css("table.fingerprint-contexts tbody td a")
      expect(links.map(&:text)).to eq(["users#show", "users#index", "courses#show", "grades#index", "api#list",
                                       "login#new"])
      show = query_of(links.first["href"])
      expect(links.first["href"]).to start_with("/fingerprints/#{users_id}?")
      expect(show).to include("context[]" => "users#show#", "project[]" => "canvas", "environment[]" => "production",
                              "cluster[]" => "13", "range" => "3h", "bucket" => "10m", "role[]" => "primary")
      expect(show.keys).not_to include("fingerprint_id")
    end

    it "links the untagged context and a job context" do
      ReportFixture.add_untagged!(users_id, 150)
      doc = doc_for

      untagged = doc.at_css("table.fingerprint-contexts td a .context-untagged").ancestors("a").first
      expect(query_of(untagged["href"])["context[]"]).to eq("##")
    end

    it "draws only that context's calls, and says which context is showing" do
      unfiltered = doc_for
      expect(series_total(unfiltered, "calls")).to eq(1100)
      expect(unfiltered.at_css(".context-filter")).to be_nil

      doc = doc_for(context: "users#show#")

      expect(response).to have_http_status(:ok)
      expect(series_total(doc, "calls")).to eq(600)
      expect(doc.css("svg[data-series=calls] circle.chart-point").size).to eq(18)
      # Its share of each window's time by calls, so under the all-contexts total of 530.
      expect(series_total(doc, "total_ms")).to be_within(0.01).of(150 + 50 + (250.0 * 200 / 491))
      active = doc.at_css(".context-filter")
      expect(active.text).to include("users#show")
      show_all = active.css("a").find { |a| a.text == "Show all contexts" }
      expect(query_of(show_all["href"]).keys).not_to include("context[]")
      expect(show_all["href"]).to include("bucket=10m", "range=3h")
      # The contexts list stays whole so another one can be picked.
      expect(doc.css("table.fingerprint-contexts tbody tr").size).to eq(7)
      expect(doc.css("table.fingerprint-contexts tbody tr[aria-current]").map(&:text).join).to include("users#show")
    end

    it "keeps the filter in the zoom URL, the reset link, the form and the table link" do
      doc = doc_for(context: "users#show#", range: "custom", from: (Time.now.utc - 2.hours).strftime("%Y-%m-%dT%H:%M"),
                    to: (Time.now.utc - 1.hour).strftime("%Y-%m-%dT%H:%M"), reset_range: "3h")

      expect(query_of(doc.at_css("[data-controller=chart]")["data-chart-zoom-url-value"])).to include("context[]" => "users#show#")
      expect(query_of(doc.at_css("a.chart-reset")["href"])).to include("context[]" => "users#show#", "range" => "3h")
      expect(doc.at_css("form.report-form input[type=hidden][name='context[]']")["value"]).to eq("users#show#")
      expect(query_of(doc.at_css("a[href*='fingerprint_timeseries']")["href"])).to include("context[]" => "users#show#")
      # A link that changes the contexts keeps the zoomed range and where to
      # reset to. users#show is the window's only context, so it's not a
      # link in the table; its remove link is one.
      link = query_of(doc.at_css(".context-filter a[aria-label='Remove context users#show']")["href"])
      expect(link).to include("range" => "custom", "reset_range" => "3h")
    end

    it "does not set a context field on the form when none is picked" do
      expect(doc_for.at_css("form.report-form input[name='context[]']")).to be_nil
    end

    it "draws zeros, not an error, for a context this fingerprint never ran in" do
      doc = doc_for(context: "programs#show#")

      expect(response).to have_http_status(:ok)
      expect(series_total(doc, "calls")).to eq(0)
      expect(doc.at_css(".context-filter").text).to include("programs#show")
    end

    it "runs the per-context series instead of the all-contexts one" do
      files = []
      allow(ReportSql).to receive(:read).and_wrap_original do |original, file|
        files << file
        original.call(file)
      end

      get "/fingerprints/#{users_id}", params: params.merge(context: "users#show#")

      expect(files).to eq(%w[fingerprint_context_timeseries.sql fingerprint_contexts.sql fingerprint_sources.sql
                             fingerprint_all_sources.sql])
    end

    it "filters the time series report as a table too" do
      get "/reports", params: params.merge(report: "fingerprint_timeseries", fingerprint_id: users_id, context: "users#show#")

      expect(response).to have_http_status(:ok)
      calls = Nokogiri::HTML5(response.body).css("table.report tbody tr").map { |row| row.css("td")[2].text.delete(",").to_i }
      expect(calls.sum).to eq(600)
    end

    it "answers 422 for a malformed context, running nothing" do
      expect(ReportRunner).not_to receive(:new)
      { "users" => "Context must look like controller#action#job_tag", "users#show" => "Context must look like controller#action#job_tag", "x#y" => "Context must look like controller#action#job_tag",
        "a#b#c" * 400 => "Context is invalid", "users#show#\u0001" => "Context is invalid",
        "users#show#\n" => "Context is invalid" }.each do |context, message|
        get "/fingerprints/#{users_id}", params: params.merge(context: context)

        expect(response).to have_http_status(:unprocessable_content), context.inspect
        expect(response.body).to include(message), context.inspect
        expect(response.body).not_to include("timeseries-chart"), context.inspect
      end
    end

    it "answers 422 for a context that isn't a string or a list of them" do
      expect(ReportRunner).not_to receive(:new)

      get "/fingerprints/#{users_id}?#{params.to_query}&context[a]=users%23show%23"
      expect(response).to have_http_status(:unprocessable_content)
    end

    it "takes a list of contexts, as context[]" do
      get "/fingerprints/#{users_id}?#{params.to_query}&context[]=users%23show%23&context[]=users%23index%23"
      expect(response).to have_http_status(:ok)
      expect(series_total(Nokogiri::HTML5(response.body), "calls")).to eq(720)
    end

    it "round-trips a job tag that contains a #" do
      ReportFixture.add_context!(users_id, 7, job_tag: "Sync#All")
      doc = doc_for

      link = doc.css("table.fingerprint-contexts tbody td a").find { |a| a.text == "Sync#All" }
      expect(query_of(link["href"])["context[]"]).to eq("##Sync#All")
      follow = doc_for(context: "##Sync#All")
      expect(series_total(follow, "calls")).to eq(7)
      expect(follow.at_css(".context-filter").text).to include("Sync#All")
      expect(follow.css("table.fingerprint-contexts tbody tr[aria-current]").size).to eq(1)
      expect(series_total(doc_for(context: "a#b#c#d"), "calls")).to eq(0)
    end

    it "shows a context it can't name in a link as plain text" do
      ReportFixture.add_context!(users_id, 3, controller: "we#ird", action: "show")
      doc = doc_for

      row = doc.css("table.fingerprint-contexts tbody tr").find { |tr| tr.text.include?("we#ird#show") }
      expect(row).to be_present
      expect(row.at_css("a")).to be_nil
      expect(doc.css("table.fingerprint-contexts tbody td a").size).to eq(7)
    end

    it "treats a blank context as no filter" do
      doc = doc_for(context: "")

      expect(response).to have_http_status(:ok)
      expect(series_total(doc, "calls")).to eq(1100)
      expect(doc.at_css(".context-filter")).to be_nil
    end
  end
end
