require "rails_helper"

# Feeds SQL injection payloads into every report parameter. Reports run with
# bound parameters only, so a payload is just a value: it may fail validation
# (4xx with a friendly message) or match nothing (200), but it never runs as
# SQL, never errors with a 500, and never changes or leaks data.
module ReportInjectionSpec
  PAYLOADS = [
    "'; drop table rotten.fingerprints; --",
    "' or '1'='1",
    "canvas' or 1=1 --",
    "13); select pg_sleep(5); --",
    "1; select pg_sleep(5)",
    "$1",
    "'||(select string_agg(email, ',') from rotten.users)||'",
    "\\x00",
    "nul\u0000byte",
    "1 union select 1,2,3,4,5,6",
    "1e400",
    "-1",
    "99999999999999999999999",
    "1 hour'::interval; drop table rotten.events; --",
    "Robert\u2019); DROP TABLE Students;--",
    "x" * 5000
  ].freeze

  STRUCTURED = [
    ["'; drop table rotten.events; --"],
    { "a" => "' or 1=1 --" }
  ].freeze

  PARAMS = %w[project environment cluster role range from to sort dir primary_role replica_role fingerprint_id bucket sort_report].freeze
end

RSpec.describe "Report SQL injection", type: :request do
  payloads = ReportInjectionSpec::PAYLOADS
  structured = ReportInjectionSpec::STRUCTURED
  params = ReportInjectionSpec::PARAMS

  let(:base) do
    { "project" => "canvas", "environment" => "production", "cluster" => "13", "range" => "3h",
      "fingerprint_id" => @fixture.fingerprint_ids.fetch("users").to_s }
  end

  before do
    User.delete_all
    @fixture = ReportFixture.seed!
    user = User.create!(email: "injection@example.com", name: "Viewer", role: "viewer", active: true)
    # Never signed in, so its email shows on no page unless a payload leaks it.
    User.create!(email: "injection-bystander@example.com", name: "Bystander", role: "admin", active: true)
    post "/__test/sign_in", params: { user_id: user.id }
    @counts = ReportFixture.table_counts
  end

  def expect_safe(path, params)
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    get path, params: params
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

    expect([200, 400, 404, 422]).to include(response.status), "#{path} #{params.inspect} -> #{response.status}"
    expect(elapsed).to be < 4, "#{path} #{params.inspect} took #{elapsed}s"
    expect(response.body).not_to include("injection-bystander@example.com")
    # The top bar shows the signed-in user's email; nothing else may.
    page = Nokogiri::HTML5(response.body)
    page.css("header.topbar").remove
    expect(page.to_html).not_to include("injection@example.com")
    expect(response.body).not_to include("programs", "SyncLearners")
  end

  Report.all.each do |report|
    describe report.key do
      params.each do |param|
        it "treats every payload in #{param} as a value" do
          (payloads + structured).each do |payload|
            expect_safe("/reports", base.merge("report" => report.key, param => payload))
          end
          expect(ReportFixture.table_counts).to eq(@counts)
        end
      end

      it "treats payloads in the custom range as values" do
        payloads.each do |payload|
          expect_safe("/reports", base.merge("report" => report.key, "range" => "custom", "from" => payload, "to" => "2026-01-01T00:00"))
          expect_safe("/reports", base.merge("report" => report.key, "range" => "custom", "from" => "2026-01-01T00:00", "to" => payload))
        end
        expect(ReportFixture.table_counts).to eq(@counts)
      end
    end
  end

  describe "fingerprint detail" do
    let(:path) { "/fingerprints/#{@fixture.fingerprint_ids.fetch('users')}" }

    params.each do |param|
      it "treats every payload in #{param} as a value" do
        (payloads + structured).each do |payload|
          expect_safe(path, base.merge(param => payload))
        end
        expect(ReportFixture.table_counts).to eq(@counts)
      end
    end

    it "treats payloads in the custom range as values" do
      payloads.each do |payload|
        expect_safe(path, base.merge("range" => "custom", "from" => payload, "to" => "2026-01-01T00:00"))
        expect_safe(path, base.merge("range" => "custom", "from" => "2026-01-01T00:00", "to" => payload))
      end
      expect(ReportFixture.table_counts).to eq(@counts)
    end

    it "404s from the controller for every payload in the fingerprint id" do
      expect(Fingerprint).to receive(:lookup).exactly(payloads.size).times.and_call_original

      payloads.each do |payload|
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        get "/fingerprints/#{ERB::Util.url_encode(payload)}", params: base
        elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

        expect(response).to have_http_status(:not_found), "#{payload.inspect} -> #{response.status}"
        expect(response.body).to eq("Not found"), "#{payload.inspect} wasn't rejected by the controller"
        expect(elapsed).to be < 4
      end
      expect(ReportFixture.table_counts).to eq(@counts)
    end
  end

  it "doesn't leak another project's rows through the source fields" do
    get "/reports", params: base.merge("report" => "top_by_calls", "project" => "canvas' or project = 'bridge")

    expect(response).to have_http_status(:unprocessable_content)
    expect(response.body).not_to include("programs")
  end

  it "rejects every payload in the report parameter with a 422, running nothing" do
    expect(ReportRunner).not_to receive(:new)

    (payloads + structured).each do |payload|
      expect_safe("/reports", base.merge("report" => payload))
      expect(response).to have_http_status(:unprocessable_content), payload.inspect
      expect(response.body).to include("Report is not one of the choices")
    end
    expect(ReportFixture.table_counts).to eq(@counts)
  end

  it "keeps payloads as values through the /reports/:id redirect" do
    (payloads + structured).each do |payload|
      get "/reports/top_by_calls", params: base.merge("project" => payload)
      expect(response).to have_http_status(:moved_permanently)
      expect(URI(response.location).path).to eq("/reports")

      expect_safe(response.location, {})
    end
    expect(ReportFixture.table_counts).to eq(@counts)
  end

  it "404s for payloads in the report name" do
    payloads.first(6).each do |payload|
      get "/reports/#{ERB::Util.url_encode(payload)}"

      expect(response).to have_http_status(:not_found)
    end
    expect(ReportFixture.table_counts).to eq(@counts)
  end

  it "rejects a sort column that isn't in the report's whitelist" do
    get "/reports", params: base.merge("report" => "top_by_calls", "sort" => "calls; drop table rotten.events")

    expect(response).to have_http_status(:unprocessable_content)
    expect(response.body).to include("Sort is not a column of this report")
  end
end
