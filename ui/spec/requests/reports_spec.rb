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

  it "requires login for the index and every report" do
    get "/reports"
    expect(response).to redirect_to("/login")

    Report.all.each do |report|
      get "/reports/#{report.key}", params: source_params
      expect(response).to redirect_to("/login")
    end
  end

  it "serves every report to a viewer" do
    sign_in

    get "/reports"
    expect(response).to have_http_status(:ok)

    Report.all.each do |report|
      get "/reports/#{report.key}", params: source_params.merge(fingerprint_id: "1")
      expect(response).to have_http_status(:ok), "#{report.key}: #{response.status}"
    end
  end

  it "shows the form without running anything until a source is picked" do
    sign_in
    expect(ReportRunner).not_to receive(:new)

    get "/reports/top_by_calls"

    expect(response).to have_http_status(:ok)
    expect(response.body).to include("Run report")
    expect(response.body).not_to include('class="report"')
  end

  it "renders every source choice, and the catalog for the picker to narrow them, so the form works without JavaScript" do
    sign_in

    get "/reports/top_by_calls", params: source_params

    form = Nokogiri::HTML(response.body)
    options = ->(name) { form.css("select[name=#{name}] option").map { |o| o["value"] } }
    expect(options.("project")).to eq(%w[bridge canvas])
    expect(options.("environment")).to eq(%w[production])
    expect(options.("cluster")).to eq(%w[13 7])
    expect(options.("role")).to eq(["", "primary", "replica"])
    picker = form.at_css("[data-controller=source-picker]")
    expect(JSON.parse(picker["data-source-picker-catalog-value"])).to eq(LogicalSource.catalog)
    expect(picker.css("select").map { |s| s["data-source-picker-target"] }).to eq(%w[project environment cluster role])
  end

  it "404s for a report that doesn't exist" do
    sign_in

    get "/reports/nope"

    expect(response).to have_http_status(:not_found)
  end

  it "answers 422 with a friendly message for invalid parameters" do
    sign_in

    get "/reports/top_by_calls", params: source_params.merge(range: "forever")

    expect(response).to have_http_status(:unprocessable_content)
    expect(response.body).to include("Time range is not one of the choices")
  end

  it "answers 422 for a custom range whose end isn't after its start" do
    sign_in

    get "/reports/top_by_calls", params: source_params.merge(range: "custom", from: "2026-01-02T00:00", to: "2026-01-01T00:00")

    expect(response).to have_http_status(:unprocessable_content)
    expect(response.body).to include("To must be after From")
  end

  it "answers 422 for a time series with too many buckets" do
    sign_in

    get "/reports/fingerprint_timeseries",
        params: source_params.merge(range: "7d", bucket: "1m", fingerprint_id: "1")

    expect(response).to have_http_status(:unprocessable_content)
    expect(response.body).to include("too many buckets")
  end

  it "answers 503 with a friendly message when a report times out" do
    sign_in
    allow(ReportRunner).to receive(:new).and_wrap_original do |original, **|
      original.call(timeout_ms: 50)
    end
    allow(ReportSql).to receive(:read).and_call_original
    allow(ReportSql).to receive(:read).with("outliers.sql")
                                      .and_return("select pg_sleep(1), $1::text, $2::text, $3::text, $4::timestamptz, " \
                                                  "$5::timestamptz, $6::integer, $7::float8, $8::integer, $9::float8, $10::text")

    get "/reports/outliers", params: source_params

    expect(response).to have_http_status(:service_unavailable)
    expect(response.body).to include("took longer than")
  end
end
