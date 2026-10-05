require "rails_helper"

RSpec.describe "Reports", type: :request do
  let(:source_params) { { project: "canvas", environment: "production", cluster: "13", range: "3h" } }

  before do
    User.delete_all
    ReportFixture.seed!
  end

  def sign_in(role: "viewer")
    user = User.create!(email: "#{role}-reports@example.com", name: "User", role: role, active: true)
    post "/__test/sign_in", params: { user_id: user.id }
  end

  def doc = Nokogiri::HTML(response.body)

  it "requires login for the workbench and every report" do
    get "/reports"
    expect(response).to redirect_to("/login")

    Report.all.each do |report|
      get "/reports", params: source_params.merge(report: report.key)
      expect(response).to redirect_to("/login")
      get "/reports/#{report.key}", params: source_params
      expect(response).to redirect_to("/login")
    end
  end

  it "serves every report to a viewer" do
    sign_in

    get "/reports"
    expect(response).to have_http_status(:ok)

    Report.all.each do |report|
      get "/reports", params: source_params.merge(report: report.key, fingerprint_id: "1")
      expect(response).to have_http_status(:ok), "#{report.key}: #{response.status}"
      expect(doc.at_css("h2.report-title").text).to eq(report.title)
    end
  end

  it "shows the form without running anything until a source is picked" do
    sign_in
    expect(ReportRunner).not_to receive(:new)

    get "/reports", params: { report: "top_by_calls" }

    expect(response).to have_http_status(:ok)
    expect(response.body).to include("Run report")
    expect(response.body).not_to include('class="report"')
    expect(doc.at_css("input[name=report][value=top_by_calls]")["checked"]).to be_present
  end

  it "lists every report as a choice with its title and description, the first one picked by default" do
    sign_in

    get "/reports"

    choices = doc.css("input[type=radio][name=report]")
    expect(choices.map { |c| c["value"] }).to eq(Report.all.map(&:key))
    expect(choices.select { |c| c["checked"] }.map { |c| c["value"] }).to eq([Report.all.first.key])
    Report.all.each do |report|
      label = doc.at_css("label[for=report_#{report.key}]")
      expect(label.text).to include(report.title, report.description)
    end
  end

  it "renders every source choice, and the catalog for the picker to narrow them, so the form works without JavaScript" do
    sign_in

    get "/reports", params: source_params.merge(report: "top_by_calls")

    options = ->(name) { doc.css("select[name=#{name}] option").map { |o| o["value"] } }
    expect(options.("project")).to eq(%w[bridge canvas])
    expect(options.("environment")).to eq(%w[production])
    expect(options.("cluster")).to eq(%w[13 7])
    expect(options.("role")).to eq(["", "primary", "replica"])
    picker = doc.at_css("[data-controller~=source-picker]")
    expect(JSON.parse(picker["data-source-picker-catalog-value"])).to eq(LogicalSource.catalog)
    expect(picker.css("select").map { |s| s["data-source-picker-target"] }).to eq(%w[project environment cluster role])
  end

  it "renders every report's fields, tagged with the reports that use them, and none disabled, so the form works without JavaScript" do
    sign_in

    get "/reports", params: source_params.merge(report: "top_by_calls")

    reports_for = ->(name) { doc.at_css("[name=#{name}]").ancestors("[data-report-chooser-target=field]").first["data-reports"].split }
    expect(reports_for.("role")).to eq(Report.all.select(&:role_filter?).map(&:key))
    expect(reports_for.("primary_role")).to eq(%w[replica_utilization_by_controller_action replica_utilization_by_job])
    expect(reports_for.("replica_role")).to eq(%w[replica_utilization_by_controller_action replica_utilization_by_job])
    expect(reports_for.("fingerprint_id")).to eq(%w[fingerprint_timeseries])
    expect(reports_for.("bucket")).to eq(%w[fingerprint_timeseries])
    expect(doc.css("form.report-form [disabled]")).to be_empty
    expect(doc.css("form.report-form [hidden]")).to be_empty
    %w[from to].each do |name|
      expect(doc.at_css("[name=#{name}]").ancestors(".custom-range").first.text).to include("Custom range only")
    end
  end

  it "ignores the fields that don't apply to the picked report, as a form without JavaScript sends them all" do
    sign_in

    get "/reports", params: source_params.merge(report: "top_by_calls", fingerprint_id: "", bucket: "x" * 1001,
                                                primary_role: "nope", replica_role: "nope\u0001")

    expect(response).to have_http_status(:ok)
    expect(doc.css("table.report tbody tr").size).to eq(3)

    get "/reports", params: source_params.merge(report: "replica_utilization_by_job", role: "nope", fingerprint_id: "x")

    expect(response).to have_http_status(:ok)
    expect(doc.css("table.report tbody tr")).not_to be_empty
  end

  it "rejects a report that doesn't exist with a friendly message" do
    sign_in
    expect(ReportRunner).not_to receive(:new)

    ["nope", "", "fingerprint_contexts"].each do |key|
      get "/reports", params: source_params.merge(report: key)

      expect(response).to have_http_status(:unprocessable_content), key.inspect
      expect(response.body).to include("Report is not one of the choices")
    end
  end

  describe "/reports/:id, for old bookmarks and links" do
    it "redirects to the workbench with the report and every parameter kept" do
      sign_in

      get "/reports/top_by_calls", params: source_params.merge(role: "replica", sort: "calls", dir: "asc")

      expect(response).to have_http_status(:moved_permanently)
      location = URI(response.location)
      expect(location.path).to eq("/reports")
      expect(Rack::Utils.parse_query(location.query)).to eq(
        "report" => "top_by_calls", "project" => "canvas", "environment" => "production", "cluster" => "13",
        "range" => "3h", "role" => "replica", "sort" => "calls", "dir" => "asc"
      )

      follow_redirect!
      expect(response).to have_http_status(:ok)
      expect(doc.at_css("th[data-column=calls]")["aria-sort"]).to eq("ascending")
    end

    it "uses the report in the path over a report parameter" do
      sign_in

      get "/reports/outliers", params: { report: "top_by_calls" }

      expect(Rack::Utils.parse_query(URI(response.location).query)).to eq("report" => "outliers")
    end

    it "404s for a report that doesn't exist" do
      sign_in

      get "/reports/nope"

      expect(response).to have_http_status(:not_found)
    end
  end

  describe "the resolved time window" do
    def bound_window
      windows = []
      allow_any_instance_of(ReportRunner).to receive(:run).and_wrap_original do |original, sql, binds|
        windows << binds[3, 2].map { |time| Time.iso8601(time) }
        original.call(sql, binds)
      end
      yield
      expect(windows.uniq.size).to eq(1)
      windows.first
    end

    it "states the preset window the report ran over" do
      sign_in

      from, to = bound_window { get "/reports", params: source_params.merge(report: "top_by_calls") }

      expect(to - from).to eq(3.hours)
      text = doc.at_css(".report-window").text.squish
      expect(text).to eq("Window: #{from.strftime('%Y-%m-%d %H:%M')} to #{to.strftime(from.to_date == to.to_date ? '%H:%M' : '%Y-%m-%d %H:%M')} UTC (last 3 hours)")
    end

    it "states a custom window, with both dates when they differ" do
      sign_in

      from, to = bound_window do
        get "/reports", params: source_params.merge(report: "outliers", range: "custom", from: "2026-10-03T22:15", to: "2026-10-04T01:05")
      end

      expect([from, to]).to eq([Time.utc(2026, 10, 3, 22, 15), Time.utc(2026, 10, 4, 1, 5)])
      expect(doc.at_css(".report-window").text.squish).to eq("Window: 2026-10-03 22:15 to 2026-10-04 01:05 UTC (custom range)")
      expect(doc.at_css(".report-ignored")).to be_nil
    end

    it "says so when From and To are sent with a preset range, and ignores them" do
      sign_in

      from, to = bound_window do
        get "/reports", params: source_params.merge(report: "top_by_calls", range: "6h", from: "2026-01-01T00:00", to: "2026-01-02T00:00")
      end

      expect(to - from).to eq(6.hours)
      expect(doc.at_css(".report-window").text).to include("(last 6 hours)")
      expect(doc.at_css(".report-ignored").text.squish).to eq(
        "From and To were ignored: they apply only when Time range is Custom."
      )
    end

    it "carries the server's time and each preset's length for the Custom pre-fill" do
      sign_in
      travel_to Time.utc(2026, 10, 4, 9, 35, 20)

      get "/reports", params: source_params.merge(report: "top_by_calls")

      window = doc.at_css("[data-controller~=time-window]")
      expect(window["data-time-window-now-value"]).to eq("2026-10-04T09:35:20Z")
      seconds = doc.css("select[name=range] option").to_h { |o| [o["value"], o["data-seconds"]] }
      expect(seconds).to eq("1h" => "3600", "3h" => "10800", "6h" => "21600", "24h" => "86400", "7d" => "604800", "custom" => nil)
    end
  end

  describe "switching reports" do
    it "has no report tabs above the results: the form's report chooser is the only switch" do
      sign_in

      get "/reports", params: source_params.merge(report: "top_by_calls", role: "replica", sort: "calls", dir: "asc")

      expect(response).to have_http_status(:ok)
      results = doc.at_css("section.results")
      expect(results).not_to be_nil
      expect(doc.css("nav.report-tabs, .report-tab")).to be_empty
      expect(doc.css("nav[aria-label='Reports on this dataset']")).to be_empty
      report_links = results.css("a[href]").select { |a| URI(a["href"]).path == "/reports" }
      expect(report_links.map { |a| Rack::Utils.parse_query(URI(a["href"]).query)["report"] }.uniq).to eq(["top_by_calls"])
      expect(doc.css("form.report-form input[type=radio][name=report]").map { |r| r["value"] }).to eq(Report.all.map(&:key))
    end

    it "asks for a fingerprint ID rather than failing when the time series is opened without one, as from an old bookmark" do
      sign_in
      expect(ReportRunner).not_to receive(:new)

      get "/reports", params: source_params.merge(report: "fingerprint_timeseries")

      expect(response).to have_http_status(:ok)
      expect(doc.at_css(".report-needs-input").text).to include("Enter a fingerprint ID")
      expect(doc.css(".report-errors")).to be_empty
    end
  end

  describe "the dataset role on reports that don't filter by it" do
    it "keeps a valid role in the utilization report's form, so switching back doesn't widen the dataset" do
      sign_in

      get "/reports", params: source_params.merge(report: "replica_utilization_by_job", role: "replica")

      expect(response).to have_http_status(:ok)
      expect(doc.at_css("select[name=role] option[selected]")["value"]).to eq("replica")
    end

    it "drops a role the source doesn't have, rather than failing or carrying it" do
      sign_in

      get "/reports", params: source_params.merge(report: "replica_utilization_by_job", role: "nope")

      expect(response).to have_http_status(:ok)
      expect(doc.css(".report-errors")).to be_empty
      expect(doc.css("select[name=role] option[selected]").map { |o| o["value"] }).to eq([""])
    end

    it "keeps the role field sent while hidden for the reports that don't filter by it" do
      sign_in

      get "/reports"

      role = doc.at_css("[name=role]").ancestors("[data-report-chooser-target=field]").first
      expect(role["data-report-chooser-keep"]).to eq("true")
      %w[primary_role replica_role fingerprint_id bucket].each do |name|
        expect(doc.at_css("[name=#{name}]").ancestors("[data-report-chooser-target=field]").first["data-report-chooser-keep"]).to be_nil
      end
    end
  end

  describe "a sort from another report, as a form without JavaScript sends it" do
    def sorted_columns = doc.css("th[aria-sort]").map { |th| th["data-column"] }

    it "marks the form's sort with the report it came from" do
      sign_in

      get "/reports", params: source_params.merge(report: "top_by_calls", sort: "calls", dir: "asc")

      form = doc.at_css("form.report-form")
      expect(form.at_css("input[type=hidden][name=sort]")["value"]).to eq("calls")
      expect(form.at_css("input[type=hidden][name=sort_report]")["value"]).to eq("top_by_calls")
    end

    it "drops a sort on a column the picked report doesn't have" do
      sign_in

      get "/reports", params: source_params.merge(report: "replica_utilization_by_job", sort: "calls", dir: "asc",
                                                  sort_report: "top_by_calls")

      expect(response).to have_http_status(:ok)
      expect(sorted_columns).to be_empty
      expect(doc.css("input[name=sort], input[name=sort_report]")).to be_empty
    end

    it "drops a sort on a column the picked report shares" do
      sign_in

      get "/reports", params: source_params.merge(report: "top_by_total_time", sort: "calls", dir: "asc",
                                                  sort_report: "top_by_calls")

      expect(response).to have_http_status(:ok)
      expect(sorted_columns).to be_empty
      expect(doc.css("td[data-column=calls]").map(&:text)).to eq(%w[85 1,100 100])
    end

    it "keeps a sort from the picked report itself" do
      sign_in

      get "/reports", params: source_params.merge(report: "top_by_total_time", sort: "calls", dir: "asc",
                                                  sort_report: "top_by_total_time")

      expect(sorted_columns).to eq(["calls"])
      expect(doc.css("td[data-column=calls]").map(&:text)).to eq(%w[85 100 1,100])
    end

    it "still rejects a sort that isn't a column of the picked report it came from" do
      sign_in

      get "/reports", params: source_params.merge(report: "replica_utilization_by_job", sort: "calls",
                                                  sort_report: "replica_utilization_by_job")

      expect(response).to have_http_status(:unprocessable_content)
      expect(response.body).to include("Sort is not a column of this report")
    end

    it "rejects a marker that isn't a report" do
      sign_in

      ["nope", "fingerprint_contexts", ""].each do |marker|
        get "/reports", params: source_params.merge(report: "top_by_calls", sort: "calls", sort_report: marker)

        expect(response).to have_http_status(:unprocessable_content), marker.inspect
        expect(response.body).to include("Sort report is not one of the choices")
      end
    end
  end

  it "keeps the sort links on the workbench" do
    sign_in

    get "/reports", params: source_params.merge(report: "top_by_calls")

    href = doc.at_css("th[data-column=calls] a")["href"]
    expect(URI(href).path).to eq("/reports")
    expect(Rack::Utils.parse_query(URI(href).query)).to include("report" => "top_by_calls", "sort" => "calls", "dir" => "desc")
  end

  it "answers 422 with a friendly message for invalid parameters" do
    sign_in

    get "/reports", params: source_params.merge(report: "top_by_calls", range: "forever")

    expect(response).to have_http_status(:unprocessable_content)
    expect(response.body).to include("Time range is not one of the choices")
  end

  it "answers 422 for a custom range whose end isn't after its start" do
    sign_in

    get "/reports", params: source_params.merge(report: "top_by_calls", range: "custom", from: "2026-01-02T00:00", to: "2026-01-01T00:00")

    expect(response).to have_http_status(:unprocessable_content)
    expect(response.body).to include("To must be after From")
  end

  it "answers 422 for a time series with too many buckets" do
    sign_in

    get "/reports", params: source_params.merge(report: "fingerprint_timeseries", range: "7d", bucket: "1m", fingerprint_id: "1")

    expect(response).to have_http_status(:unprocessable_content)
    expect(response.body).to include("too many buckets")
  end

  it "answers 422 for a time series with a blank fingerprint ID" do
    sign_in

    get "/reports", params: source_params.merge(report: "fingerprint_timeseries", fingerprint_id: "")

    expect(response).to have_http_status(:unprocessable_content)
    expect(response.body).to include("Fingerprint ID must be a positive whole number")
  end

  it "answers 503 with a friendly message when a report times out" do
    sign_in
    allow(ReportRunner).to receive(:new).and_wrap_original do |original, **|
      original.call(timeout_ms: 50)
    end
    allow(ReportSql).to receive(:read).and_call_original
    allow(ReportSql).to receive(:read).with("outliers.sql")
                                      .and_return("select pg_sleep(1), $1::text, $2::text, $3::text, $4::timestamptz, " \
                                                  "$5::timestamptz, $6::integer, $7::float8, $8::integer, $9::float8, $10::text, $11::text")

    get "/reports", params: source_params.merge(report: "outliers")

    expect(response).to have_http_status(:service_unavailable)
    expect(response.body).to include("took longer than")
    expect(doc.at_css(".report-window")).to be_present
  end
end
