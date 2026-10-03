require Rails.root.join("lib/rotten_ui/oidc_config")
require Rails.root.join("lib/rotten_ui/fake_login")

auth_mode = Rails.configuration.x.auth_mode
fake_login = RottenUi::FakeLogin.enabled?(env: ENV, rails_env: Rails.env, auth_mode: auth_mode)

# In oidc mode the app refuses to boot without its IdP settings. The one
# exception is the development-only fake login, which works offline.
oidc = if auth_mode == "oidc" && !fake_login
  RottenUi::OidcConfig.fetch!(ENV)
else
  RottenUi::OidcConfig.from_env(ENV)
end

# The test environment always installs the strategy so specs can drive it in
# OmniAuth test mode; the callback controller still 404s unless in oidc mode.
oidc_strategy = Rails.env.test? || (auth_mode == "oidc" && oidc.complete?)

Rails.configuration.x.oidc = oidc
Rails.configuration.x.fake_login = fake_login
Rails.configuration.x.oidc_strategy = oidc_strategy

OmniAuth.config.logger = Rails.logger

# Send every failure to one generic page. The error type is logged, sanitized,
# because it can come from the callback's query string.
OmniAuth.config.on_failure = lambda do |env|
  error = env["omniauth.error.type"].to_s.gsub(/[^A-Za-z0-9_.-]/, "")[0, 64]
  Rails.logger.warn("OIDC sign-in failed: #{error.presence || 'unknown'}")
  [302, { "location" => "#{env['SCRIPT_NAME']}/auth/failure", "content-type" => "text/html" }, []]
end

if oidc_strategy
  require Rails.root.join("lib/rotten_ui/oidc_strategy")

  Rails.application.config.middleware.use OmniAuth::Builder do
    provider RottenUi::OidcStrategy, **oidc.strategy_options
  end
end
