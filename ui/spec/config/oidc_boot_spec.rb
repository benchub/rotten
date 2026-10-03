require "rails_helper"
require "open3"

# Boots the app in a child process, the way a deploy would, so these cover
# the initializers and route drawing rather than a stubbed copy of them.
RSpec.describe "OIDC boot configuration" do
  let(:report_script) do
    <<~RUBY
      route = begin
        Rails.application.routes.recognize_path("/auth/fake/admin", method: :post)
      rescue ActionController::RoutingError
        nil
      end

      # Walk the real middleware stack to the strategy instance OmniAuth built.
      scope = nil
      if defined?(RottenUi::OidcStrategy)
        app = Rails.application.app
        100.times do
          break if app.nil?

          app = app.to_app if app.is_a?(OmniAuth::Builder)
          if app.is_a?(RottenUi::OidcStrategy)
            scope = app.options.scope
            break
          end
          app = app.instance_variable_get(:@app)
        end
      end

      puts "REPORT " + {
        fake_login: Rails.configuration.x.fake_login,
        fake_route: !route.nil?,
        omniauth: Rails.application.middleware.include?(OmniAuth::Builder),
        scope: scope
      }.to_json
    RUBY
  end

  let(:oidc_env) do
    {
      "OIDC_ISSUER" => "https://idp.example.test",
      "OIDC_CLIENT_ID" => "boot-client",
      "OIDC_CLIENT_SECRET" => "boot-secret-value"
    }
  end

  def boot(rails_env, env)
    base = {
      "RAILS_ENV" => rails_env,
      "DATABASE_URL" => "postgresql://boot.invalid/none",
      "SECRET_KEY_BASE_DUMMY" => "1",
      "ROTTEN_UI_HOSTS" => "rotten.example.test",
      "OIDC_ISSUER" => nil,
      "OIDC_CLIENT_ID" => nil,
      "OIDC_CLIENT_SECRET" => nil,
      "OIDC_GROUPS_CLAIM" => nil,
      "OIDC_REDIRECT_URI" => nil,
      "OIDC_SCOPES" => nil,
      "ROTTEN_UI_VIEWER_GROUP" => nil,
      "ROTTEN_UI_ADMIN_GROUP" => nil,
      "OMNIAUTH_FAKE" => nil
    }
    stdout, stderr, status = Open3.capture3(base.merge(env), "bin/rails", "runner", report_script, chdir: Rails.root.to_s)
    report = stdout.lines.grep(/\AREPORT /).last
    [status, report && JSON.parse(report.delete_prefix("REPORT ")), stdout + stderr]
  end

  it "refuses to boot in oidc mode when an OIDC value is missing, without echoing secrets" do
    status, report, output = boot("production", oidc_env.except("OIDC_ISSUER").merge("ROTTEN_UI_AUTH" => "oidc"))

    expect(status).not_to be_success
    expect(report).to be_nil
    expect(output).to include("ROTTEN_UI_AUTH=oidc requires OIDC_ISSUER")
    expect(output).not_to include("boot-secret-value")
  end

  it "boots in oidc mode with every OIDC value, and OMNIAUTH_FAKE stays inert in production" do
    status, report, output = boot("production", oidc_env.merge("ROTTEN_UI_AUTH" => "oidc", "OMNIAUTH_FAKE" => "1"))

    expect(status).to be_success, output
    expect(report).to eq("fake_login" => false, "fake_route" => false, "omniauth" => true,
                         "scope" => %w[openid email profile])
  end

  it "asks the identity provider for the scopes in OIDC_SCOPES" do
    status, report, output = boot("production", oidc_env.merge("ROTTEN_UI_AUTH" => "oidc",
                                                               "OIDC_SCOPES" => "email groups offline_access"))

    expect(status).to be_success, output
    expect(report["scope"]).to eq(%w[openid email groups offline_access])
  end

  it "does not install OmniAuth in password mode in production" do
    status, report, output = boot("production", { "ROTTEN_UI_AUTH" => "password" })

    expect(status).to be_success, output
    expect(report).to include("omniauth" => false, "fake_route" => false)
  end

  it "keeps OMNIAUTH_FAKE inert in the test environment" do
    status, report, output = boot("test", oidc_env.merge("ROTTEN_UI_AUTH" => "oidc", "OMNIAUTH_FAKE" => "1"))

    expect(status).to be_success, output
    expect(report).to include("fake_login" => false, "fake_route" => false)
  end

  it "enables the fake login in development without real OIDC values" do
    status, report, output = boot("development", { "ROTTEN_UI_AUTH" => "oidc", "OMNIAUTH_FAKE" => "1" })

    expect(status).to be_success, output
    expect(report).to eq("fake_login" => true, "fake_route" => true, "omniauth" => false, "scope" => nil)
  end
end
