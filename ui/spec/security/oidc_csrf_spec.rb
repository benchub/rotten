require "rails_helper"

RSpec.describe "OIDC request phase CSRF protection", type: :request do
  before do
    User.delete_all
    use_oidc_mode
    mock_oidc_auth(uid: "sub-csrf", groups: [])
  end

  around do |example|
    original = ActionController::Base.allow_forgery_protection
    ActionController::Base.allow_forgery_protection = true
    example.run
  ensure
    ActionController::Base.allow_forgery_protection = original
  end

  it "refuses to start a login from a POST without an authenticity token" do
    post "/auth/openid_connect"

    expect(response).to redirect_to("/auth/failure")
  end

  it "starts a login from a POST carrying the session's authenticity token" do
    get "/login"
    token = Nokogiri::HTML(response.body).at_css("form[action='/auth/openid_connect'] input[name='authenticity_token']")["value"]

    post "/auth/openid_connect", params: { authenticity_token: token }

    expect(response).to redirect_to(%r{/auth/openid_connect/callback\z})
  end

  it "does not start a login from a GET" do
    get "/auth/openid_connect"

    expect(response).to have_http_status(:not_found)
  end
end
