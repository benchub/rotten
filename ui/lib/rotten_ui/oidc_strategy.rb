require "omniauth_openid_connect"

module RottenUi
  # The stock strategy reads redirect_uri from client_options, which every
  # request shares. Deriving it per request here avoids mutating that shared
  # state, and drops the stock behavior of appending a caller-supplied
  # redirect_uri param.
  class OidcStrategy < OmniAuth::Strategies::OpenIDConnect
    private

    def redirect_uri
      client_options.redirect_uri.presence || "#{full_host}#{callback_path}"
    end
  end
end
