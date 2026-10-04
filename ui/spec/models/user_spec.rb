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

    it "ends the user's sessions by bumping session_generation, and keeps the new value on the object" do
      expect { user.change_password("brand-new-password-2", "brand-new-password-2") }
        .to change { User.find(user.id).session_generation }.by(1)

      expect(user.session_generation).to eq(User.find(user.id).session_generation)
      expect(user).not_to be_changed
    end

    it "leaves session_generation alone when the change is refused" do
      expect { user.change_password("short", "short") }.not_to(change { User.find(user.id).session_generation })
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

  describe "#revoke_sessions!" do
    it "starts users at generation 0" do
      expect(User.create!(email: "gen@example.test", role: "viewer").session_generation).to eq(0)
    end

    # Each bump is one UPDATE ... SET session_generation = session_generation
    # + 1, so a stale copy of the row can't overwrite another bump.
    it "bumps atomically in the database, so stale copies of the user don't lose a bump" do
      user = User.create!(email: "gen@example.test", role: "viewer")
      first = User.find(user.id)
      second = User.find(user.id)

      expect(first.revoke_sessions!).to eq(1)
      expect(second.revoke_sessions!).to eq(2)

      expect(second.session_generation).to eq(2)
      expect(second).not_to be_changed
      expect(user.reload.session_generation).to eq(2)
    end

    it "counts every bump made at the same time" do
      user = User.create!(email: "gen@example.test", role: "viewer")

      threads = Array.new(4) do
        Thread.new do
          ActiveRecord::Base.connection_pool.with_connection do
            copy = User.find(user.id)
            5.times { copy.revoke_sessions! }
          end
        end
      end
      threads.each(&:join)

      expect(user.reload.session_generation).to eq(20)
    end

    it "touches only the given user" do
      user = User.create!(email: "gen@example.test", role: "viewer")
      other = User.create!(email: "other@example.test", role: "viewer")

      user.revoke_sessions!

      expect(other.reload.session_generation).to eq(0)
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
