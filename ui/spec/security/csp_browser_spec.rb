require "rails_helper"

# The CSP header checks in headers_spec.rb read the policy; these run it in
# Chromium, so a policy that breaks importmap or Turbo, lets injected inline
# script run, or blocks the redirect to the identity provider fails here.
RSpec.describe "Content-Security-Policy in the browser", type: :system do
  let(:password) { "csp-browser-spec-password" }

  before do
    User.delete_all
    browser_logs
  end

  after { OmniAuth.config.full_host = nil }

  # Reading the browser log empties it, so each call returns only what's new.
  def browser_logs
    page.driver.browser.logs.get(:browser).map(&:message)
  end

  def csp_violations(logs = browser_logs)
    logs.grep(/Content Security Policy/i)
  end

  def port
    Capybara.current_session.server.port
  end

  it "runs importmap and Turbo and loads the stylesheet with no violations, signed out and in" do
    create_password_user(email: "csp.viewer@example.test", password: password)

    visit "/login"
    expect(page.evaluate_script("typeof window.Turbo")).to eq("object")
    stylesheets = page.evaluate_script(<<~JS)
      Array.from(document.styleSheets).filter((s) => s.href && new URL(s.href).origin === location.origin).length
    JS
    expect(stylesheets).to be >= 1

    fill_in "Email", with: "csp.viewer@example.test"
    fill_in "Password", with: password
    click_button "Sign in"
    expect(page).to have_text("Signed in as csp.viewer@example.test")
    expect(page.evaluate_script("typeof window.Turbo")).to eq("object")

    click_button "Log out"
    expect(page).to have_text("Signed out")

    expect(csp_violations).to be_empty
  end

  it "blocks an injected inline script" do
    visit "/login"
    page.execute_script(<<~JS)
      const s = document.createElement("script");
      s.textContent = "window.cspInjected = true";
      document.head.appendChild(s);
    JS

    expect(page.evaluate_script("window.cspInjected === true")).to be(false)
    expect(csp_violations.join("\n")).to include("script-src")
  end

  context "in oidc mode, where Sign in redirects to another origin" do
    # The browser is on 127.0.0.1 and OmniAuth's redirect goes to localhost,
    # the same server under another origin, standing in for the IdP.
    before do
      OmniAuth.config.full_host = "http://localhost:#{port}"
      mock_oidc_auth(uid: "sub-csp", email: "csp.oidc@example.test", groups: ["rotten-viewers"])
    end

    it "follows the redirect when that origin is the issuer's" do
      use_oidc_mode(viewer_group: "rotten-viewers", issuer: "http://localhost:#{port}/realms/rotten")

      visit "http://127.0.0.1:#{port}/login"
      click_button "Sign in"

      expect(page).to have_text("Signed in as csp.oidc@example.test")
      expect(URI(page.current_url).host).to eq("localhost")
      expect(csp_violations).to be_empty
    end

    it "follows the redirect when that origin is in ROTTEN_UI_CSP_FORM_ACTION_ORIGINS, as for a federated IdP" do
      original = Rails.configuration.x.csp_form_action_origins
      Rails.configuration.x.csp_form_action_origins = ["http://localhost:#{port}"]
      use_oidc_mode(viewer_group: "rotten-viewers", issuer: "https://idp.example.test")

      visit "http://127.0.0.1:#{port}/login"
      click_button "Sign in"

      expect(page).to have_text("Signed in as csp.oidc@example.test")
      expect(csp_violations).to be_empty
    ensure
      Rails.configuration.x.csp_form_action_origins = original
    end

    it "blocks the redirect to any other origin" do
      use_oidc_mode(viewer_group: "rotten-viewers", issuer: "https://idp.example.test")

      visit "http://127.0.0.1:#{port}/login"
      click_button "Sign in"

      logs = []
      Timeout.timeout(Capybara.default_max_wait_time) do
        until csp_violations(logs).any?
          logs.concat(browser_logs)
          sleep 0.1
        end
      end
      expect(csp_violations(logs).join("\n")).to include("form-action")
      expect(page).to have_no_text("Signed in as")
    end
  end
end
