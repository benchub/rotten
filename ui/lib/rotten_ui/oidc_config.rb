module RottenUi
  # OIDC settings, all from env. Nothing org-specific has a default here; the
  # deploy repo supplies the values.
  OidcConfig = Data.define(:issuer, :client_id, :client_secret, :groups_claim, :viewer_group, :admin_group,
                           :redirect_uri, :scopes) do
    def self.required = %w[OIDC_ISSUER OIDC_CLIENT_ID OIDC_CLIENT_SECRET]

    def self.default_scopes = %w[openid email profile]

    def self.fetch!(env = ENV)
      missing = required.select { |name| env[name].to_s.strip.empty? }
      raise "ROTTEN_UI_AUTH=oidc requires #{missing.join(', ')}" if missing.any?

      from_env(env)
    end

    def self.from_env(env = ENV)
      value = ->(name) { env[name].to_s.strip.presence }

      new(
        issuer: value.call("OIDC_ISSUER"),
        client_id: value.call("OIDC_CLIENT_ID"),
        client_secret: value.call("OIDC_CLIENT_SECRET"),
        groups_claim: value.call("OIDC_GROUPS_CLAIM") || "groups",
        viewer_group: value.call("ROTTEN_UI_VIEWER_GROUP"),
        admin_group: value.call("ROTTEN_UI_ADMIN_GROUP"),
        redirect_uri: value.call("OIDC_REDIRECT_URI"),
        scopes: value.call("OIDC_SCOPES")&.split
      )
    end

    # openid is always requested, first, because without it this isn't OIDC.
    def initialize(redirect_uri: nil, scopes: nil, **rest)
      scopes = self.class.default_scopes if scopes.blank?
      super(redirect_uri: redirect_uri, scopes: (["openid"] + scopes).uniq.freeze, **rest)
    end

    def complete?
      [issuer, client_id, client_secret].all?(&:present?)
    end

    # What users.provider holds for users from this issuer. Including the
    # issuer means a sub from one issuer never matches a user from another.
    def provider
      "openid_connect:#{issuer}" if issuer.present?
    end

    # scheme://host[:port] of the issuer, or nil if it isn't an http(s) URL.
    def origin
      uri = URI.parse(issuer.to_s)
      return nil unless uri.is_a?(URI::HTTP) && uri.host.present?

      port = uri.port == uri.default_port ? "" : ":#{uri.port}"
      "#{uri.scheme}://#{uri.host.downcase}#{port}"
    rescue URI::InvalidURIError
      nil
    end

    def strategy_options
      {
        name: "openid_connect",
        issuer: issuer,
        discovery: true,
        response_type: :code,
        pkce: true,
        scope: scopes,
        client_options: {
          identifier: client_id,
          secret: client_secret,
          redirect_uri: redirect_uri
        }
      }
    end

    def inspect
      "#<#{self.class.name} issuer=#{issuer.inspect} client_id=#{client_id.inspect} client_secret=[FILTERED]>"
    end
    alias_method :to_s, :inspect
  end
end
