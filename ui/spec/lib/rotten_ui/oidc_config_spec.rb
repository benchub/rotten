require "rails_helper"

RSpec.describe RottenUi::OidcConfig do
  let(:complete_env) do
    {
      "OIDC_ISSUER" => "https://idp.example.test",
      "OIDC_CLIENT_ID" => "client-id",
      "OIDC_CLIENT_SECRET" => "client-secret-value"
    }
  end

  describe ".fetch!" do
    it "reads every OIDC value from env" do
      config = described_class.fetch!(complete_env.merge(
        "OIDC_GROUPS_CLAIM" => "roles",
        "ROTTEN_UI_VIEWER_GROUP" => "viewers",
        "ROTTEN_UI_ADMIN_GROUP" => "admins",
        "OIDC_REDIRECT_URI" => "https://rotten.example.test/auth/openid_connect/callback"
      ))

      expect(config).to have_attributes(
        issuer: "https://idp.example.test",
        client_id: "client-id",
        client_secret: "client-secret-value",
        groups_claim: "roles",
        viewer_group: "viewers",
        admin_group: "admins",
        redirect_uri: "https://rotten.example.test/auth/openid_connect/callback"
      )
      expect(config).to be_complete
    end

    %w[OIDC_ISSUER OIDC_CLIENT_ID OIDC_CLIENT_SECRET].each do |name|
      it "fails when #{name} is missing" do
        expect { described_class.fetch!(complete_env.except(name)) }
          .to raise_error(RuntimeError, "ROTTEN_UI_AUTH=oidc requires #{name}")
      end

      it "fails when #{name} is blank" do
        expect { described_class.fetch!(complete_env.merge(name => "  ")) }
          .to raise_error(RuntimeError, "ROTTEN_UI_AUTH=oidc requires #{name}")
      end
    end

    it "names every missing value without echoing any configured secret" do
      expect { described_class.fetch!({ "OIDC_CLIENT_SECRET" => "client-secret-value" }) }
        .to raise_error(RuntimeError, "ROTTEN_UI_AUTH=oidc requires OIDC_ISSUER, OIDC_CLIENT_ID") { |error|
          expect(error.message).not_to include("client-secret-value")
        }
    end

    it "defaults the groups claim to groups and leaves both role groups unset" do
      config = described_class.fetch!(complete_env)

      expect(config.groups_claim).to eq("groups")
      expect(config.viewer_group).to be_nil
      expect(config.admin_group).to be_nil
      expect(config.redirect_uri).to be_nil
    end

    it "treats blank role groups as unset" do
      config = described_class.fetch!(complete_env.merge("ROTTEN_UI_VIEWER_GROUP" => "", "ROTTEN_UI_ADMIN_GROUP" => " "))

      expect(config.viewer_group).to be_nil
      expect(config.admin_group).to be_nil
    end
  end

  describe ".from_env" do
    it "does not require the OIDC values" do
      config = described_class.from_env({})

      expect(config).not_to be_complete
      expect(config.groups_claim).to eq("groups")
    end
  end

  describe "scopes" do
    it "defaults to openid email profile, without groups" do
      expect(described_class.fetch!(complete_env).scopes).to eq(%w[openid email profile])
    end

    it "reads space-separated scopes from OIDC_SCOPES" do
      config = described_class.fetch!(complete_env.merge("OIDC_SCOPES" => " openid  email\tprofile groups "))

      expect(config.scopes).to eq(%w[openid email profile groups])
    end

    it "always includes openid, first, and drops duplicates" do
      config = described_class.fetch!(complete_env.merge("OIDC_SCOPES" => "email groups email"))

      expect(config.scopes).to eq(%w[openid email groups])
    end

    it "falls back to the default when OIDC_SCOPES is blank" do
      expect(described_class.fetch!(complete_env.merge("OIDC_SCOPES" => "  ")).scopes).to eq(%w[openid email profile])
    end
  end

  describe "#provider" do
    it "namespaces the issuer so users from different issuers stay apart" do
      expect(described_class.fetch!(complete_env).provider).to eq("openid_connect:https://idp.example.test")
    end

    it "is nil without an issuer" do
      expect(described_class.from_env({}).provider).to be_nil
    end
  end

  describe "#strategy_options" do
    it "passes the configured scopes and client settings to the strategy" do
      config = described_class.fetch!(complete_env.merge("OIDC_SCOPES" => "openid groups"))

      expect(config.strategy_options).to include(
        name: "openid_connect", issuer: "https://idp.example.test", discovery: true, response_type: :code, pkce: true,
        scope: %w[openid groups]
      )
      expect(config.strategy_options[:client_options]).to include(identifier: "client-id", secret: "client-secret-value")
    end
  end
end
