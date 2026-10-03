module RottenUi
  class AuthMode
    VALID = %w[oidc password].freeze
    MESSAGE = "ROTTEN_UI_AUTH must be set to oidc or password"

    def self.fetch!(env = ENV)
      mode = env["ROTTEN_UI_AUTH"]
      raise MESSAGE unless VALID.include?(mode)

      mode
    end
  end
end
