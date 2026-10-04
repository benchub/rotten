require "rails_helper"

# The strict CSP has no 'unsafe-inline' for styles, so a style attribute or a
# <style> element would be blocked in the browser and the page would render
# unstyled. Every page, signed in or out, is checked here: none may carry a
# style attribute, a <style> element, an inline event handler, or a script
# without src other than importmap's nonced ones.
RSpec.describe "No inline styles on any page", :api_keys, type: :request do
  let(:password) { "inline-style-spec-password" }

  before do
    User.delete_all
    @fixture = ReportFixture.seed!
  end

  def sign_in(user)
    post "/__test/sign_in", params: { user_id: user.id }
  end

  def expect_no_inline_style(label)
    doc = Nokogiri::HTML5(response.body)
    styled = doc.css("[style]").map { |node| node.to_html[0, 120] }
    expect(styled).to be_empty, "#{label}: elements with a style attribute: #{styled.inspect}"
    expect(doc.css("style")).to be_empty, "#{label}: has a <style> element"
    handlers = doc.css("*").flat_map { |node| node.attributes.keys }.grep(/\Aon/i)
    expect(handlers).to be_empty, "#{label}: inline event handlers #{handlers.inspect}"
    expect(doc.css("script:not([src])").map { |s| s["type"] }.sort).to eq(%w[importmap module]), label
    # The page is styled at all: the stylesheet that replaces any inline styling is linked.
    expect(doc.css("link[rel=stylesheet]")).not_to be_empty, label
  end

  def get_ok(path, status: :ok)
    get path
    expect(response).to have_http_status(status), "GET #{path}: got #{response.status}"
    expect_no_inline_style("GET #{path}")
  end

  it "has none on the signed-in pages a viewer sees, with and without results and errors" do
    viewer = create_password_user(email: "inline.viewer@example.test", password: password)
    sign_in(viewer)
    run = "project=canvas&environment=production&cluster=13&range=3h"
    fingerprint = @fixture.fingerprint_ids.fetch("users")

    get_ok "/"
    get_ok "/password"
    get_ok "/reports"
    get_ok "/reports?report=top_by_calls&#{run}"
    get_ok "/reports?report=outliers&#{run}"
    get_ok "/reports?report=replica_utilization_by_job&#{run}"
    get_ok "/reports?report=fingerprint_timeseries&#{run}"
    get_ok "/reports?report=fingerprint_timeseries&fingerprint_id=#{fingerprint}&#{run}"
    get_ok "/reports?report=top_by_calls&#{run}&range=custom&from=2026-01-02T00:00&to=2026-01-01T00:00", status: :unprocessable_content
    get_ok "/fingerprints/#{fingerprint}?#{run}"
    get_ok "/fingerprints/#{fingerprint}?project=nowhere&environment=production&cluster=13&range=3h",
           status: :unprocessable_content
  end

  it "has none on the admin pages, including the one-time pass key page and form errors" do
    admin = create_password_user(email: "inline.admin@example.test", password: password, role: "admin")
    sign_in(admin)
    owner_create_api_key(name: "existing")
    owner_insert_audit_row(action: "api_key.create", actor_email: "keys@example.test", target_type: "api_key",
                           target_id: "1", details: { "name" => "existing" })

    get_ok "/admin"
    get_ok "/admin/keys"
    get_ok "/admin/keys/new"
    get_ok "/admin/audit"

    post "/admin/keys", params: { api_key: { name: "inline-db1", fqdn: "db1.example.test" } }
    expect(response).to have_http_status(:created)
    expect_no_inline_style("POST /admin/keys")

    post "/admin/keys", params: { api_key: { name: "bad name", fqdn: "db1..example.test" } }
    expect(response).to have_http_status(:unprocessable_content)
    expect_no_inline_style("POST /admin/keys with errors")
  end

  it "has none on the password sign-in page, with and without an error" do
    get_ok "/login"

    post "/login", params: { email: "nobody@example.test", password: "wrong" }
    expect_no_inline_style("POST /login")
  end

  context "in oidc mode" do
    around do |example|
      fake_login = Rails.configuration.x.fake_login
      Rails.application.routes.disable_clear_and_finalize = true
      Rails.application.routes.draw do
        post "auth/fake/:persona" => "fake_sessions#create", as: :fake_login
      end
      example.run
    ensure
      Rails.application.routes.disable_clear_and_finalize = false
      Rails.configuration.x.fake_login = fake_login
      Rails.application.reload_routes!
    end

    before { use_oidc_mode(viewer_group: "rotten-viewers") }

    it "has none on the sign-in page with the fake personas, or the denied page" do
      Rails.configuration.x.fake_login = true
      get_ok "/login"
      expect(response.body).to include("Sign in as fake viewer")

      mock_oidc_auth(uid: "sub-inline-outsider", email: "outsider@example.test", groups: ["elsewhere"])
      oidc_sign_in
      expect(response).to have_http_status(:forbidden)
      expect_no_inline_style("denied")
    end
  end
end
