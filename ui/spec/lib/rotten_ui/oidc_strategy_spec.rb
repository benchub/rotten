require "rails_helper"

RSpec.describe RottenUi::OidcStrategy do
  def redirect_uri_for(url, configured: nil)
    strategy = described_class.new(->(_env) { [200, {}, []] }, name: "openid_connect",
                                                              client_options: { redirect_uri: configured })
    env = Rack::MockRequest.env_for(url)
    env["rack.session"] = {}
    strategy.instance_variable_set(:@env, env)
    strategy.send(:redirect_uri)
  end

  it "derives the callback URL from the request, without its query string or a redirect_uri param" do
    uri = redirect_uri_for("https://rotten.example.test/auth/openid_connect/callback?code=abc&state=xyz&redirect_uri=https://evil.example")

    expect(uri).to eq("https://rotten.example.test/auth/openid_connect/callback")
  end

  it "uses OIDC_REDIRECT_URI when it is configured" do
    uri = redirect_uri_for("http://internal:3000/auth/openid_connect", configured: "https://rotten.example.test/auth/openid_connect/callback")

    expect(uri).to eq("https://rotten.example.test/auth/openid_connect/callback")
  end
end
