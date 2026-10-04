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

  it "rejects a new password shorter than the minimum" do
    user = User.new(email: "short@example.test", role: "viewer", provider: User::PASSWORD_PROVIDER,
                    password: "a" * (User::MIN_PASSWORD_LENGTH - 1))

    expect(user).not_to be_valid
    expect(user.errors.of_kind?(:password, :too_short)).to be(true)
  end

  it "accepts a password of exactly the minimum length" do
    user = User.new(email: "min@example.test", role: "viewer", provider: User::PASSWORD_PROVIDER,
                    password: "a" * User::MIN_PASSWORD_LENGTH)

    expect(user).to be_valid
  end

  it "accepts the passwords users:create generates" do
    expect(UserAdmin::PASSWORD_LENGTH).to be >= User::MIN_PASSWORD_LENGTH
  end

  it "rejects a confirmation that doesn't match, when one is given" do
    user = User.new(email: "confirm@example.test", role: "viewer", provider: User::PASSWORD_PROVIDER,
                    password: "long-enough-password", password_confirmation: "long-enough-passwore")

    expect(user).not_to be_valid
    expect(user.errors.of_kind?(:password_confirmation, :confirmation)).to be(true)
  end

  describe "#change_password" do
    let(:user) do
      User.create!(email: "changer@example.test", role: "viewer", provider: User::PASSWORD_PROVIDER,
                   password: "original-password-1")
    end

    it "saves a new password that meets the policy" do
      expect(user.change_password("brand-new-password-2", "brand-new-password-2")).to be(true)

      expect(user.reload.authenticate("brand-new-password-2")).to eq(user)
      expect(user.authenticate("original-password-1")).to be(false)
    end

    [
      ["a blank password", "", ""],
      ["a nil password", nil, nil],
      ["a non-string password", ["brand-new-password-2"], ["brand-new-password-2"]],
      ["a too-short password", "short", "short"],
      ["a too-long password", "a" * 73, "a" * 73],
      ["a mismatched confirmation", "brand-new-password-2", "brand-new-password-3"],
      ["a missing confirmation", "brand-new-password-2", nil]
    ].each do |label, password, confirmation|
      it "refuses #{label}, with an error, and keeps the old password" do
        expect(user.change_password(password, confirmation)).to be(false)

        expect(user.errors).not_to be_empty
        expect(user.reload.authenticate("original-password-1")).to eq(user)
      end
    end

    it "refuses an OIDC user" do
      sso = User.create!(email: "sso-change@example.test", role: "viewer", provider: "openid_connect:https://idp",
                         provider_uid: "sub")

      expect(sso.change_password("brand-new-password-2", "brand-new-password-2")).to be(false)
      expect(sso.reload.password_digest).to be_nil
    end
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
