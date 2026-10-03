module RottenUi
  # Offline sign-in for local development: OMNIAUTH_FAKE=1 with
  # ROTTEN_UI_AUTH=oidc. It is inert in every other environment, so the route
  # isn't drawn and the controller refuses it.
  module FakeLogin
    PERSONAS = {
      "viewer" => { name: "Fake Viewer", email: "viewer@fake-login.invalid" },
      "admin" => { name: "Fake Admin", email: "admin@fake-login.invalid" }
    }.freeze

    def self.enabled?(env: ENV, rails_env: Rails.env, auth_mode: Rails.configuration.x.auth_mode)
      rails_env.to_s == "development" && auth_mode == "oidc" && env["OMNIAUTH_FAKE"] == "1"
    end

    # Personas carry groups from ROTTEN_UI_VIEWER_GROUP and
    # ROTTEN_UI_ADMIN_GROUP, so they go through the same role rules as a real
    # login. With no admin group set, the admin persona is a viewer.
    def self.auth_hash(persona, config)
      details = PERSONAS.fetch(persona)
      groups = [config.viewer_group]
      groups << config.admin_group if persona == "admin"
      groups = groups.compact

      OmniAuth::AuthHash.new(
        provider: "fake",
        uid: "fake-#{persona}",
        info: { name: details[:name], email: details[:email], email_verified: true },
        extra: { raw_info: { config.groups_claim => groups } }
      )
    end
  end
end
