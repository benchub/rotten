require "rails_helper"

# Response headers in the test env. spec/security/production_boot_spec.rb
# checks the ones that only production sets (HSTS, Secure cookies, hosts).
RSpec.describe "Security headers", type: :request do
  def csp_directives(header = response.headers["content-security-policy"])
    expect(header).to be_present
    header.split(";").to_h do |directive|
      name, *sources = directive.split
      [name, sources]
    end
  end

  def script_nonces(body = response.body)
    Nokogiri::HTML(body).css("script").map { |script| script["nonce"] }
  end

  before { User.delete_all }

  describe "Content-Security-Policy" do
    it "is strict on the login page" do
      get "/login"

      directives = csp_directives
      expect(directives).to include(
        "default-src" => ["'self'"],
        "base-uri" => ["'self'"],
        "object-src" => ["'none'"],
        "frame-ancestors" => ["'none'"],
        "form-action" => ["'self'"],
        "img-src" => ["'self'", "data:"]
      )
      expect(directives["script-src"]).to match([ "'self'", a_string_matching(/\A'nonce-[A-Za-z0-9+\/=]+'\z/) ])
      expect(directives["style-src"]).to match([ "'self'", a_string_matching(/\A'nonce-[A-Za-z0-9+\/=]+'\z/) ])
    end

    it "allows no inline code, eval, or whole schemes anywhere" do
      get "/login"

      sources = csp_directives.values.flatten
      expect(sources).not_to include("'unsafe-inline'", "'unsafe-eval'", "'unsafe-hashes'", "*", "http:", "https:")
    end

    it "nonces every script tag with the header's nonce, which changes on every request" do
      get "/login"
      nonce = csp_directives["script-src"].last[/'nonce-(.+)'/, 1]

      expect(script_nonces).to be_present
      expect(script_nonces).to all(eq(nonce))
      expect(Nokogiri::HTML(response.body).at_css("meta[name='csp-nonce']")["content"]).to eq(nonce)

      get "/login"
      expect(csp_directives["script-src"].last[/'nonce-(.+)'/, 1]).not_to eq(nonce)
    end

    it "doesn't use the session ID as the nonce" do
      get "/login"
      nonce = csp_directives["script-src"].last[/'nonce-(.+)'/, 1]

      expect(nonce).not_to eq(session.id.to_s)
    end

    it "lets forms post to the OIDC issuer's origin in oidc mode, for the redirect to the identity provider" do
      use_oidc_mode(issuer: "https://idp.example.test:8443/oauth2/default")

      get "/login"

      expect(csp_directives["form-action"]).to eq(["'self'", "https://idp.example.test:8443"])
    end

    it "lets forms post to the origins in ROTTEN_UI_CSP_FORM_ACTION_ORIGINS too, for federated sign-in" do
      original = Rails.configuration.x.csp_form_action_origins
      Rails.configuration.x.csp_form_action_origins = ["https://login.example.test", "https://broker.example.test:8443"]
      use_oidc_mode(issuer: "https://idp.example.test/oauth2/default")

      get "/login"

      expect(csp_directives["form-action"]).to eq(
        ["'self'", "https://idp.example.test", "https://login.example.test", "https://broker.example.test:8443"]
      )
    ensure
      Rails.configuration.x.csp_form_action_origins = original
    end

    it "boots with no extra form-action origins in the test env" do
      expect(Rails.configuration.x.csp_form_action_origins).to eq([])
    end

    it "is sent on pages for signed-in users and on refusals" do
      user = create_password_user(email: "csp@example.test", password: "csp-spec-password-123")
      post "/__test/sign_in", params: { user_id: user.id }

      get "/"
      expect(csp_directives).to include("default-src" => ["'self'"])

      get "/admin"
      expect(response).to have_http_status(:forbidden)
      expect(response.headers["content-security-policy"]).to be_present
    end
  end

  describe "other headers" do
    it "forbids framing, sniffing and leaking the full referrer, and turns off powerful features" do
      get "/login"

      expect(response.headers).to include(
        "x-frame-options" => "DENY",
        "x-content-type-options" => "nosniff",
        "referrer-policy" => "strict-origin-when-cross-origin",
        "x-permitted-cross-domain-policies" => "none"
      )
      permissions = response.headers["permissions-policy"].to_s.split(",").map(&:strip)
      expect(permissions).to include("camera=()", "microphone=()", "geolocation=()", "payment=()", "usb=()")
    end
  end
end
