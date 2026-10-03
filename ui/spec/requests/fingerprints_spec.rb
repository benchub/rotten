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

  it "doesn't list the fingerprint detail queries as reports" do
    sign_in

    get "/reports/fingerprint_contexts", params: source_params
    expect(response).to have_http_status(:not_found)

    get "/reports/fingerprint_sources", params: source_params
    expect(response).to have_http_status(:not_found)
  end
end
