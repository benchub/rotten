OmniAuth.config.test_mode = true

module OidcHelpers
  TEST_ISSUER = "https://idp.example.test".freeze

  # What a user signed in through TEST_ISSUER has in users.provider.
  def oidc_provider(issuer = TEST_ISSUER)
    "openid_connect:#{issuer}"
  end

  # Switches the running app into oidc mode with test-only IdP values. The
  # around hook below restores the boot-time configuration after each example.
  def use_oidc_mode(viewer_group: nil, admin_group: nil, groups_claim: "groups", issuer: TEST_ISSUER)
    Rails.configuration.x.auth_mode = "oidc"
    Rails.configuration.x.oidc = RottenUi::OidcConfig.new(
      issuer: issuer,
      client_id: "test-client",
      client_secret: "test-secret",
      groups_claim: groups_claim,
      viewer_group: viewer_group,
      admin_group: admin_group,
      redirect_uri: nil
    )
  end

  # Builds the auth hash OmniAuth test mode hands to the callback. Pass
  # groups: :absent to leave the groups claim out of raw_info entirely, and
  # email_verified: :absent to leave that claim out everywhere.
  def mock_oidc_auth(uid: "sub-1", email: "person@example.test", name: "Test Person", groups: :absent,
                     claim: "groups", email_verified: true, raw_info: {})
    raw = { "sub" => uid, "email" => email }
    info = { email: email, name: name }
    unless email_verified == :absent
      raw["email_verified"] = email_verified
      info[:email_verified] = email_verified
    end
    raw = raw.merge(raw_info)
    raw[claim] = groups unless groups == :absent

    OmniAuth.config.mock_auth[:openid_connect] = OmniAuth::AuthHash.new(
      provider: "openid_connect",
      uid: uid,
      info: info,
      extra: { raw_info: raw }
    )
  end

  def oidc_sign_in
    post "/auth/openid_connect"
    expect(response).to redirect_to(%r{/auth/openid_connect/callback\z})
    get "/auth/openid_connect/callback"
  end

  def body_text
    Nokogiri::HTML(response.body).text
  end
end

RSpec.configure do |config|
  config.include OidcHelpers

  config.around do |example|
    auth_mode = Rails.configuration.x.auth_mode
    oidc = Rails.configuration.x.oidc
    example.run
  ensure
    Rails.configuration.x.auth_mode = auth_mode
    Rails.configuration.x.oidc = oidc
    OmniAuth.config.mock_auth[:openid_connect] = nil
  end
end
