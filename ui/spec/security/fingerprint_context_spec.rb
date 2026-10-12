require "rails_helper"

# The context filter on the fingerprint page is a value like any other report
# parameter: validated before the query, bound (never put in SQL), escaped
# where it's shown, and unable to reach another source's rows.
RSpec.describe "Fingerprint context filter", type: :request do
  let(:params) { { project: "canvas", environment: "production", cluster: "13", range: "3h", bucket: "10m" } }

  before do
    User.delete_all
    @fixture = ReportFixture.seed!
    user = User.create!(email: "context-filter@example.com", name: "Viewer", role: "viewer", active: true)
    post "/__test/sign_in", params: { user_id: user.id }
    @counts = ReportFixture.table_counts
  end

  def users_path = "/fingerprints/#{@fixture.fingerprint_ids.fetch('users')}"

  def calls_total(body)
    Nokogiri::HTML5(body).css("svg[data-series=calls] circle.chart-point").sum { |point| point["data-value"].to_f }
  end

  it "rejects malformed contexts with a 422 before running any query" do
    expect(ReportRunner).not_to receive(:new)
    malformed = ["users", "users#show", "'; drop table rotten.events; --", "a#b", "\u0000##", "##\u0000",
                 "x#y#z\r\nSet-Cookie: a=b", "#{'x' * 1001}##"]

    malformed.each do |context|
      get users_path, params: params.merge(context: context)

      expect(response).to have_http_status(:unprocessable_content), context.inspect
      expect(response.body).to match(/Context (is invalid|must look like controller#action#job_tag)/), context.inspect
    end
    expect(ReportFixture.table_counts).to eq(@counts)
  end

  it "rejects arrays and hashes" do
    expect(ReportRunner).not_to receive(:new)

    get "#{users_path}?#{params.to_query}&context[]=users%23show%23"
    expect(response).to have_http_status(:unprocessable_content)
    get "#{users_path}?#{params.to_query}&context[x]=1"
    expect(response).to have_http_status(:unprocessable_content)
  end

  it "binds injection in a well-formed context as a value that matches nothing" do
    ["'; drop table rotten.events; --#x#", "x#'; select pg_sleep(5); --#", "##' or '1'='1", "%#_#%"].each do |context|
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      get users_path, params: params.merge(context: context)

      expect(response).to have_http_status(:ok), context
      expect(calls_total(response.body)).to eq(0), context
      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 4
    end
    expect(ReportFixture.table_counts).to eq(@counts)
  end

  it "shows nothing from another source for a context that belongs to it" do
    # SyncLearners ran this fingerprint on bridge 13 and programs#show ran
    # another one there; neither is a canvas 13 context of this fingerprint.
    ["##SyncLearners", "programs#show#"].each do |context|
      get users_path, params: params.merge(context: context)

      expect(response).to have_http_status(:ok)
      expect(calls_total(response.body)).to eq(0)
    end

    get users_path, params: params.merge(project: "bridge", context: "##SyncLearners")
    expect(calls_total(response.body)).to eq(25)

    # Each source and context match exactly: the untagged context isn't "any".
    get users_path, params: params.merge(project: "bridge", context: "##")
    expect(calls_total(response.body)).to eq(0)
  end

  it "escapes the context everywhere it's shown" do
    context = %(<script>alert(1)</script>#"><img src=x onerror=alert(1)>#)

    get users_path, params: params.merge(context: context)

    expect(response).to have_http_status(:ok)
    expect(response.body).not_to include("<script>alert(1)")
    expect(response.body).not_to include("<img src=x")
    page = Nokogiri::HTML5(response.body)
    expect(page.at_css("input[type=hidden][name=context]")["value"]).to eq(context)
    expect(page.at_css(".context-filter").text).to include("<script>alert(1)</script>")
  end

  it "keeps the report timeout on the page for the per-context query" do
    allow(ReportRunner).to receive(:new).and_wrap_original { |original, **| original.call(timeout_ms: 100) }
    allow(ReportSql).to receive(:read).and_call_original
    allow(ReportSql).to receive(:read).with("fingerprint_context_timeseries.sql")
                                      .and_return("select pg_sleep(2), $1::text, $2::text, $3::text, $4::bigint, " \
                                                  "$5::timestamptz, $6::timestamptz, $7::text, $8::text, " \
                                                  "$9::text, $10::text, $11::text")

    get users_path, params: params.merge(context: "users#show#")

    expect(response).to have_http_status(:service_unavailable)
  end
end
