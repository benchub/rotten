require "rails_helper"

RSpec.describe RottenUi::FakeLogin do
  let(:fake_env) { { "OMNIAUTH_FAKE" => "1" } }

  it "is enabled only in development, in oidc mode, with OMNIAUTH_FAKE=1" do
    expect(described_class.enabled?(env: fake_env, rails_env: "development", auth_mode: "oidc")).to be(true)
  end

  it "does nothing in the test environment" do
    expect(described_class.enabled?(env: fake_env, rails_env: "test", auth_mode: "oidc")).to be(false)
  end

  it "does nothing in the production environment" do
    expect(described_class.enabled?(env: fake_env, rails_env: "production", auth_mode: "oidc")).to be(false)
  end

  it "does nothing in password mode" do
    expect(described_class.enabled?(env: fake_env, rails_env: "development", auth_mode: "password")).to be(false)
  end

  it "needs OMNIAUTH_FAKE to be exactly 1" do
    %w[0 true yes].each do |value|
      expect(described_class.enabled?(env: { "OMNIAUTH_FAKE" => value }, rails_env: "development", auth_mode: "oidc"))
        .to be(false)
    end
    expect(described_class.enabled?(env: {}, rails_env: "development", auth_mode: "oidc")).to be(false)
  end

  describe ".auth_hash" do
    let(:config) do
      RottenUi::OidcConfig.new(issuer: nil, client_id: nil, client_secret: nil, groups_claim: "roles",
                               viewer_group: "dev-viewers", admin_group: "dev-admins", redirect_uri: nil)
    end

    it "gives the viewer persona only the viewer group, in the configured claim" do
      auth = described_class.auth_hash("viewer", config)

      expect(auth.provider).to eq("fake")
      expect(auth.uid).to eq("fake-viewer")
      expect(auth.extra.raw_info["roles"]).to eq(["dev-viewers"])
    end

    it "gives the admin persona the viewer and admin groups" do
      auth = described_class.auth_hash("admin", config)

      expect(auth.uid).to eq("fake-admin")
      expect(auth.extra.raw_info["roles"]).to eq(%w[dev-viewers dev-admins])
    end

    it "rejects unknown personas" do
      expect { described_class.auth_hash("root", config) }.to raise_error(KeyError)
    end
  end
end
