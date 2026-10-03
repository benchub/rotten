require "rails_helper"

RSpec.describe RottenUi::AuthMode do
  it "fails clearly when ROTTEN_UI_AUTH is missing" do
    expect { described_class.fetch!({}) }
      .to raise_error(RuntimeError, "ROTTEN_UI_AUTH must be set to oidc or password")
  end

  it "fails clearly when ROTTEN_UI_AUTH is unknown" do
    expect { described_class.fetch!({ "ROTTEN_UI_AUTH" => "saml" }) }
      .to raise_error(RuntimeError, "ROTTEN_UI_AUTH must be set to oidc or password")
  end
end
