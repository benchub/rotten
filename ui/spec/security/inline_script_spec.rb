require "rails_helper"

# The chart hover and zoom run from a Stimulus controller, never inline. The
# only script elements without src are importmap's, and they carry the CSP
# nonce; nothing has an event handler or style attribute, and there's no
# style element, so the page needs no unsafe-inline.
RSpec.describe "No inline script or style on the fingerprint and report pages", type: :request do
  before do
    User.delete_all
    @fixture = ReportFixture.seed!
    user = User.create!(email: "inline@example.com", name: "Viewer", role: "viewer", active: true)
    post "/__test/sign_in", params: { user_id: user.id }
  end

  def nonce
    response.headers["Content-Security-Policy"][/script-src[^;]*'nonce-([^']+)'/, 1]
  end

  def expect_no_inline(doc)
    inline = doc.css("script:not([src])")
    expect(inline.map { |s| s["type"] }.sort).to eq(%w[importmap module])
    expect(inline.map { |s| s["nonce"] }.uniq).to eq([nonce])
    attributes = doc.css("*").flat_map { |node| node.attributes.keys }
    expect(attributes.grep(/\Aon/i)).to be_empty
    expect(attributes).not_to include("style")
    expect(doc.css("style")).to be_empty
  end

  it "has no inline handlers or styles on the report workbench, before and after a run" do
    base = { project: "canvas", environment: "production", cluster: "13", range: "3h" }
    [{}, base.merge(report: "top_by_calls"), base.merge(report: "fingerprint_timeseries", fingerprint_id: "1")].each do |params|
      get "/reports", params: params

      expect(response).to have_http_status(:ok)
      doc = Nokogiri::HTML5(response.body)
      expect(doc.css("[data-controller~=report-chooser]")).not_to be_empty
      expect(doc.css("[data-controller~=time-window]")).not_to be_empty
      expect_no_inline(doc)
    end
  end

  it "has only nonced importmap scripts and no inline handlers or styles, zoomed or not" do
    base = { project: "canvas", environment: "production", cluster: "13", range: "3h" }
    zoomed = base.merge(range: "custom", from: (Time.now.utc - 2.hours).strftime("%Y-%m-%dT%H:%M"),
                        to: (Time.now.utc - 1.hour).strftime("%Y-%m-%dT%H:%M"), reset_range: "3h")

    [base, zoomed].each do |params|
      get "/fingerprints/#{@fixture.fingerprint_ids.fetch('users')}", params: params

      expect(response).to have_http_status(:ok)
      doc = Nokogiri::HTML5(response.body)
      expect(doc.css("svg.timeseries-chart circle.chart-point")).not_to be_empty
      expect(doc.css("[data-controller=chart]")).not_to be_empty
      expect(nonce).to be_present

      inline = doc.css("script:not([src])")
      expect(inline.map { |s| s["type"] }.sort).to eq(%w[importmap module])
      expect(inline.map { |s| s["nonce"] }.uniq).to eq([nonce])
      expect(JSON.parse(doc.at_css("script[type=importmap]").text).keys).to eq(["imports"])
      expect(JSON.parse(doc.at_css("script[type=importmap]").text)["imports"]).to include("controllers/chart_controller")
      expect(doc.at_css("script[type=module]:not([src])").text.strip).to eq('import "application"')

      attributes = doc.css("*").flat_map { |node| node.attributes.keys }
      expect(attributes.grep(/\Aon/i)).to be_empty
      expect(attributes).not_to include("style")
      expect(doc.css("style")).to be_empty
      expect(doc.xpath("//*[starts-with(translate(@href, 'JAVASCRIPT', 'javascript'), 'javascript:') or " \
                       "starts-with(translate(@src, 'JAVASCRIPT', 'javascript'), 'javascript:')]")).to be_empty
    end
  end
end
