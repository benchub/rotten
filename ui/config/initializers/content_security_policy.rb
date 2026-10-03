# Be sure to restart your server when you modify this file.

# A strict policy: only this origin's files, no inline code without the
# per-request nonce, no plugins, no framing. See
# spec/security/headers_spec.rb and spec/security/csp_browser_spec.rb.
#
# form-action also allows the OIDC issuer's origin in oidc mode: the Sign in
# form POSTs to /auth/openid_connect, which redirects to the identity
# provider, and Chrome applies form-action to that redirect too. Any origins
# in ROTTEN_UI_CSP_FORM_ACTION_ORIGINS are added, for providers that send the
# browser on to other origins; a bad entry stops the app from booting.
require Rails.root.join("lib/rotten_ui/csp_form_action_origins")

Rails.application.configure do
  config.x.csp_form_action_origins = RottenUi::CspFormActionOrigins.parse!(ENV["ROTTEN_UI_CSP_FORM_ACTION_ORIGINS"])

  config.content_security_policy do |policy|
    policy.default_src :self
    policy.base_uri :self
    policy.connect_src :self
    policy.font_src :self
    policy.form_action :self, lambda {
      issuer = Rails.configuration.x.oidc&.origin if Rails.configuration.x.auth_mode == "oidc"
      [issuer, *Rails.configuration.x.csp_form_action_origins].compact.uniq
    }
    policy.frame_ancestors :none
    policy.img_src :self, :data
    policy.object_src :none
    policy.script_src :self
    policy.style_src :self
  end

  # A fresh random nonce per request for importmap's inline scripts and
  # Turbo's progress-bar style. Not the session ID, which would put the
  # session's identifier in every page.
  config.content_security_policy_nonce_generator = ->(_request) { SecureRandom.base64(16) }
  config.content_security_policy_nonce_directives = %w[script-src style-src]
end
