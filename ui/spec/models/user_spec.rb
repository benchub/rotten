require "rails_helper"

RSpec.describe User, type: :model do
  before do
    User.delete_all
  end

  it "stores email stripped and lowercased" do
    user = User.create!(email: "  Alice@X.com ", role: "viewer")

    expect(user.reload.email).to eq("alice@x.com")
  end

  it "finds a user by email regardless of case" do
    user = User.create!(email: "Alice@X.com", role: "viewer")

    expect(User.find_by(email: "alice@x.com")).to eq(user)
    expect(User.find_by(email: "ALICE@x.COM")).to eq(user)
  end

  it "needs a password for a password user" do
    user = User.new(email: "pw@example.test", role: "viewer", provider: User::PASSWORD_PROVIDER)

    expect(user).not_to be_valid
    expect(user.errors[:password]).to be_present
  end

  it "doesn't need a password for an OIDC user" do
    user = User.new(email: "sso@example.test", role: "viewer", provider: "openid_connect:https://idp.example.test",
                    provider_uid: "sub")

    expect(user).to be_valid
    expect(user.password_digest).to be_nil
  end

  it "rejects a password longer than bcrypt can use" do
    user = User.new(email: "long@example.test", role: "viewer", provider: User::PASSWORD_PROVIDER,
                    password: "a" * 73)

    expect(user).not_to be_valid
    expect(user.errors[:password]).to be_present
  end

  it "rejects a duplicate email that differs only in case at the database" do
    User.create!(email: "alice@x.com", role: "viewer")

    expect do
      User.connection.execute("insert into users (email, role) values ('Alice@X.com', 'viewer')")
    end.to raise_error(ActiveRecord::RecordNotUnique)
  end

  describe "#credential_fingerprint" do
    it "changes when the password changes and hides the digest" do
      user = User.create!(email: "fp@example.test", role: "viewer", provider: User::PASSWORD_PROVIDER,
                          password: "first-password-123")
      first = user.credential_fingerprint

      expect(first).to be_present
      expect(first).not_to include(user.password_digest)
      expect(user.credential_fingerprint).to eq(first)

      user.update!(password: "second-password-456")

      expect(user.credential_fingerprint).not_to eq(first)
    end

    it "is nil for an OIDC user" do
      user = User.create!(email: "oidc-fp@example.test", role: "viewer", provider: "openid_connect:https://idp",
                          provider_uid: "sub")

      expect(user.credential_fingerprint).to be_nil
    end
  end
end
