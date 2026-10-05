require "rails_helper"

# The dataset's Match field: a case-insensitive Postgres regex, applied in the
# report SQL before the row limit, on the query text and the contexts.
RSpec.describe "Report match filter", type: :request do
  let(:source_params) { { project: "canvas", environment: "production", cluster: "13", range: "3h" } }

  before do
    User.delete_all
    @fixture = ReportFixture.seed!
    @needles = ReportFixture.seed_crowd!(@fixture.anchor)
    @stale = @needles.delete("stale")
    user = User.create!(email: "match@example.com", name: "Viewer", role: "viewer", active: true)
    post "/__test/sign_in", params: { user_id: user.id }
  end

  def doc = Nokogiri::HTML5(response.body)

  def fingerprint_ids
    doc.css("table.report tbody td[data-column=fingerprint_id]").map { |cell| cell.text.to_i }
  end

  def column_text(key)
    doc.css("table.report tbody td[data-column=#{key}]").map(&:text)
  end

  %w[top_by_calls top_by_total_time outliers].each do |key|
    describe key do
      it "fills the limit with the crowd when there's no match" do
        get "/reports", params: source_params.merge(report: key)

        expect(response).to have_http_status(:ok)
        expect(fingerprint_ids.size).to eq(ReportQuery::ROW_LIMIT)
        expect(fingerprint_ids & @needles.values).to be_empty
      end

      it "filters before the limit, on query text, controller#action and job tag, case-insensitively" do
        get "/reports", params: source_params.merge(report: key, match: "nEEdle")

        expect(response).to have_http_status(:ok)
        expect(fingerprint_ids).to match_array(@needles.values)
      end

      it "treats an empty match as no filter" do
        get "/reports", params: source_params.merge(report: key)
        unfiltered = fingerprint_ids

        get "/reports", params: source_params.merge(report: key, match: "")

        expect(response).to have_http_status(:ok)
        expect(fingerprint_ids).to eq(unfiltered)
      end

      it "matches a whole controller#action, anchors included" do
        get "/reports", params: source_params.merge(report: key, match: "^crowd7#index$")

        expect(column_text("example")).to eq(["select * from crowd_table_7 where id = $1"])
      end

      it "matches only the contexts in the time window" do
        get "/reports", params: source_params.merge(report: key, match: "stalectl")
        expect(response).to have_http_status(:ok)
        expect(fingerprint_ids).to eq([])

        get "/reports", params: source_params.merge(report: key, match: "stalectl", range: "7d")
        expect(fingerprint_ids).to eq([@stale])
      end
    end
  end

  describe "replica_utilization_by_controller_action" do
    it "keeps only the controller#action rows that match" do
      get "/reports", params: source_params.merge(report: "replica_utilization_by_controller_action")
      expect(column_text("controller_action").size).to be > ReportQuery::ROW_LIMIT

      get "/reports", params: source_params.merge(report: "replica_utilization_by_controller_action", match: "NEEDLE")

      expect(response).to have_http_status(:ok)
      expect(column_text("controller_action")).to eq(["needles#show"])
    end

    it "matches across the # between controller and action" do
      get "/reports", params: source_params.merge(report: "replica_utilization_by_controller_action", match: "s#sh")

      expect(column_text("controller_action")).to contain_exactly("users#show", "grades#show", "courses#show", "needles#show")
    end
  end

  describe "replica_utilization_by_job" do
    it "keeps only the job rows that match" do
      get "/reports", params: source_params.merge(report: "replica_utilization_by_job", match: "needle")

      expect(response).to have_http_status(:ok)
      expect(column_text("job_tag")).to eq(["NeedleJob"])
    end
  end

  describe "the time series" do
    it "ignores match and says so" do
      get "/reports", params: source_params.merge(report: "fingerprint_timeseries", match: "nothing-matches-this",
                                                  fingerprint_id: @fixture.fingerprint_ids.fetch("users"))

      expect(response).to have_http_status(:ok)
      expect(doc.css("table.report tbody tr")).not_to be_empty
      expect(doc.at_css(".report-ignored-match").text).to include("Match was ignored")
    end

    it "doesn't run the regex check" do
      runner = ReportRunner.new
      allow(ReportRunner).to receive(:new).and_return(runner)
      expect(runner).to receive(:run).once.and_call_original

      get "/reports", params: source_params.merge(report: "fingerprint_timeseries", match: "(",
                                                  fingerprint_id: @fixture.fingerprint_ids.fetch("users"))

      expect(response).to have_http_status(:ok)
    end
  end

  describe "validation" do
    it "answers 422 with a message for a regex Postgres can't compile, without running the report" do
      expect(ReportSql).not_to receive(:read)

      get "/reports", params: source_params.merge(report: "top_by_calls", match: "users(")

      expect(response).to have_http_status(:unprocessable_content)
      expect(doc.at_css(".report-errors").text).to include("Match is not a valid regular expression")
      expect(doc.at_css(".report-errors").text).to include("parentheses () not balanced")
      expect(doc.at_css("input[name=match]")["value"]).to eq("users(")
    end

    it "answers 422 for every report that uses match" do
      %w[top_by_calls top_by_total_time outliers replica_utilization_by_controller_action replica_utilization_by_job].each do |key|
        get "/reports", params: source_params.merge(report: key, match: "a{3,2}")

        expect(response).to have_http_status(:unprocessable_content), key
        expect(response.body).to include("Match is not a valid regular expression"), key
      end
    end

    it "answers 422 for a match over 200 characters" do
      get "/reports", params: source_params.merge(report: "top_by_calls", match: "a" * 201)

      expect(response).to have_http_status(:unprocessable_content)
      expect(response.body).to include("Match is too long (at most 200 characters)")

      get "/reports", params: source_params.merge(report: "top_by_calls", match: "a" * 200)
      expect(response).to have_http_status(:ok)
    end

    it "keeps the match exactly as typed, leading and trailing spaces included" do
      get "/reports", params: source_params.merge(report: "top_by_calls", match: " from users ")

      expect(doc.at_css("input[name=match]")["value"]).to eq(" from users ")
      expect(column_text("example")).to eq(["select * from users where id = $1"])
    end

    it "runs the regex check and the report through one runner, sharing its timeout" do
      runners = []
      allow(ReportRunner).to receive(:new).and_wrap_original { |original, **kwargs| original.call(**kwargs).tap { |r| runners << r } }

      get "/reports", params: source_params.merge(report: "top_by_calls", match: "users")

      expect(response).to have_http_status(:ok)
      expect(runners.size).to eq(1)
    end

    it "answers 503 when a pathological pattern runs past the timeout" do
      allow(ReportRunner).to receive(:new).and_wrap_original { |original, **| original.call(timeout_ms: 500) }
      allow(ReportSql).to receive(:read).and_wrap_original do |original, file|
        # Long query texts, on which this pattern's backreferences take Postgres seconds.
        original.call(file).sub("from rotten.fingerprints f", "from (select id, repeat('ab ', 3000) || normalized || 'c' as normalized from rotten.fingerprints) f")
      end

      get "/reports", params: source_params.merge(report: "top_by_calls", match: "(.*)(.*)\\2\\1c")

      expect(response).to have_http_status(:service_unavailable)
      expect(response.body).to include("took longer than")
    end
  end

  describe "a pattern that's only spaces" do
    it "filters on it rather than dropping it" do
      get "/reports", params: source_params.merge(report: "top_by_calls", match: "  ")

      expect(response).to have_http_status(:ok)
      expect(fingerprint_ids).to be_empty

      get "/reports", params: source_params.merge(report: "replica_utilization_by_controller_action", match: " ")

      expect(response).to have_http_status(:ok)
      expect(doc.css("table.report tbody td")).to be_empty
    end

    it "highlights it and keeps it in the form and the links" do
      get "/reports", params: source_params.merge(report: "top_by_calls", match: " ")

      expect(fingerprint_ids.size).to eq(ReportQuery::ROW_LIMIT)
      expect(doc.css("td[data-column=example] mark").map(&:text)).to include(" ")
      expect(doc.at_css("input[name=match]")["value"]).to eq(" ")
      link = doc.at_css("td[data-column=fingerprint_id] a")["href"]
      expect(Rack::Utils.parse_query(URI(link).query)).to include("match" => " ")
      sort = doc.at_css("th[data-column=calls] a")["href"]
      expect(Rack::Utils.parse_query(URI(sort).query)).to include("match" => " ")

      get link

      expect(response).to have_http_status(:ok)
      expect(doc.at_css("form.report-form input[type=hidden][name=match]")["value"]).to eq(" ")
    end

    it "says the time series ignored it" do
      id = @needles.fetch("needle_sql")
      get "/reports", params: source_params.merge(report: "fingerprint_timeseries", fingerprint_id: id, match: " ")

      expect(doc.at_css(".report-ignored-match")).to be_present
    end
  end

  describe "links" do
    it "keeps the match in the fingerprint links and the sort links" do
      get "/reports", params: source_params.merge(report: "top_by_calls", match: "needle")

      link = doc.at_css("td[data-column=fingerprint_id] a")["href"]
      expect(Rack::Utils.parse_query(URI(link).query)).to include("match" => "needle")
      sort = doc.at_css("th[data-column=calls] a")["href"]
      expect(Rack::Utils.parse_query(URI(sort).query)).to include("match" => "needle")
    end

    it "keeps the match on the fingerprint page: in its form, its zoom and its table link, without filtering it" do
      id = @needles.fetch("needle_sql")
      get "/fingerprints/#{id}", params: source_params.merge(match: "needle")

      expect(response).to have_http_status(:ok)
      expect(doc.at_css("form.report-form input[type=hidden][name=match]")["value"]).to eq("needle")
      zoom = doc.at_css("[data-controller=chart]")["data-chart-zoom-url-value"]
      expect(Rack::Utils.parse_query(URI(zoom).query)).to include("match" => "needle")
      table = doc.css("a").find { |a| a.text == "Time series as a table" }["href"]
      expect(Rack::Utils.parse_query(URI(table).query)).to include("match" => "needle")
    end

    it "shows the fingerprint page whatever the match, even one Postgres can't compile" do
      get "/fingerprints/#{@fixture.fingerprint_ids.fetch('users')}", params: source_params.merge(match: "(")

      expect(response).to have_http_status(:ok)
      expect(doc.css("table.fingerprint-contexts tbody tr")).not_to be_empty
    end
  end

  describe "highlighting" do
    it "marks the matches in the query text and the contexts" do
      get "/reports", params: source_params.merge(report: "top_by_calls", match: "Users")

      example = doc.at_css("td[data-column=example] .query-text")
      expect(example.css("mark").map(&:text)).to eq(["users"])
      expect(example.text).to eq("select * from users where id = $1")
      contexts = doc.at_css("td[data-column=context]")
      expect(contexts.css("mark").map(&:text)).to eq(%w[users users])
    end

    it "marks the matches in the utilization name column" do
      get "/reports", params: source_params.merge(report: "replica_utilization_by_job", match: "job$")

      expect(doc.css("td[data-column=job_tag] mark").map(&:text)).to eq(["Job"])
    end

    it "marks nothing without a match" do
      get "/reports", params: source_params.merge(report: "top_by_calls")

      expect(doc.css("table.report mark")).to be_empty
    end

    it "marks nothing for a pattern Ruby reads differently, but still filters" do
      get "/reports", params: source_params.merge(report: "top_by_calls", match: "\\mhaystack")

      expect(fingerprint_ids).to eq([@needles.fetch("needle_sql")])
      expect(doc.css("table.report mark")).to be_empty
    end
  end
end
