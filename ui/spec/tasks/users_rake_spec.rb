require "rails_helper"
require "rake"

RSpec.describe "users rake tasks" do
  before(:all) do
    Rails.application.load_tasks unless Rake::Task.task_defined?("users:create")
  end

  before do
    User.delete_all
  end

  # Runs a task the way bin/rails would, returning its exit status and output.
  def run_task(name, *args)
    task = Rake::Task[name]
    task.reenable
    out = StringIO.new
    err = StringIO.new
    status = 0
    $stdout = out
    $stderr = err
    begin
      task.invoke(*args)
    rescue SystemExit => e
      status = e.status
    ensure
      $stdout = STDOUT
      $stderr = STDERR
    end
    [status, out.string, err.string]
  end

  def printed_password(output)
    output[/^Password: (\S+)$/, 1]
  end

  describe "users:create" do
    it "creates an active password user and prints a one-time password that signs in" do
      status, out, err = run_task("users:create", " New.Viewer@Example.TEST ", "viewer")

      expect(status).to eq(0), err
      user = User.sole
      expect(user).to have_attributes(email: "new.viewer@example.test", role: "viewer", active: true,
                                      provider: User::PASSWORD_PROVIDER, provider_uid: nil)
      password = printed_password(out)
      expect(password.length).to be >= 20
      expect(out).to include("new.viewer@example.test")
      expect(user.password_digest).to be_present
      expect(user.password_digest).not_to include(password)
      expect(user.authenticate(password)).to eq(user)
    end

    it "creates an admin" do
      status, _out, err = run_task("users:create", "boss@example.test", "admin")

      expect(status).to eq(0), err
      expect(User.sole.role).to eq("admin")
    end

    it "prints a different password every time" do
      _, first, = run_task("users:create", "one@example.test", "viewer")
      _, second, = run_task("users:create", "two@example.test", "viewer")

      expect(printed_password(first)).to be_present
      expect(printed_password(first)).not_to eq(printed_password(second))
    end

    ["", "owner", "Admin", nil].each do |role|
      it "rejects the role #{role.inspect} and creates nobody" do
        status, out, err = run_task("users:create", "someone@example.test", role)

        expect(status).not_to eq(0)
        expect(err).to include("role must be viewer or admin")
        expect(printed_password(out)).to be_nil
        expect(User.count).to eq(0)
      end
    end

    [nil, "", "not-an-email", "two@@example.test", "spaced out@example.test"].each do |email|
      it "rejects the email #{email.inspect} and creates nobody" do
        status, _out, err = run_task("users:create", email, "viewer")

        expect(status).not_to eq(0)
        expect(err).to include("valid email")
        expect(User.count).to eq(0)
      end
    end

    it "rejects a duplicate email, whatever its case, with a clear message" do
      existing = User.create!(email: "taken@example.test", role: "admin", provider: User::PASSWORD_PROVIDER,
                              password: "existing-password-1234")

      status, out, err = run_task("users:create", "TAKEN@example.test", "viewer")

      expect(status).not_to eq(0)
      expect(err).to include("taken@example.test already exists")
      expect(err).not_to include("RecordNotUnique")
      expect(printed_password(out)).to be_nil
      expect(User.sole).to eq(existing)
      expect(existing.reload.role).to eq("admin")
      expect(existing.authenticate("existing-password-1234")).to eq(existing)
    end

    it "rejects an email that belongs to an OIDC user" do
      User.create!(email: "sso@example.test", role: "viewer", provider: oidc_provider, provider_uid: "sub-sso")

      status, _out, err = run_task("users:create", "sso@example.test", "viewer")

      expect(status).not_to eq(0)
      expect(err).to include("already exists")
      expect(User.sole.provider).to eq(oidc_provider)
    end

    it "reports a duplicate inserted by a concurrent run as a clear error, not RecordNotUnique" do
      User.create!(email: "race@example.test", role: "viewer")
      allow(User).to receive(:exists?).and_return(false)

      status, _out, err = run_task("users:create", "race@example.test", "viewer")

      expect(status).not_to eq(0)
      expect(err).to include("race@example.test already exists")
      expect(err).not_to include("RecordNotUnique")
      expect(User.count).to eq(1)
    end

    it "refuses to run in oidc mode" do
      use_oidc_mode

      status, out, err = run_task("users:create", "someone@example.test", "viewer")

      expect(status).not_to eq(0)
      expect(err).to include("ROTTEN_UI_AUTH=password")
      expect(printed_password(out)).to be_nil
      expect(User.count).to eq(0)
    end
  end

  describe "users:disable" do
    it "sets active to false" do
      user = User.create!(email: "leaving@example.test", role: "admin", provider: User::PASSWORD_PROVIDER,
                          password: "leaving-password-1234")

      status, out, err = run_task("users:disable", "Leaving@Example.test")

      expect(status).to eq(0), err
      expect(out).to include("Disabled leaving@example.test")
      expect(user.reload.active).to be(false)
    end

    it "also works in oidc mode, as the kill switch for OIDC users" do
      use_oidc_mode
      user = User.create!(email: "sso@example.test", role: "viewer", provider: oidc_provider, provider_uid: "sub-sso")

      status, _out, err = run_task("users:disable", "sso@example.test")

      expect(status).to eq(0), err
      expect(user.reload.active).to be(false)
    end

    it "fails clearly for an unknown email" do
      status, _out, err = run_task("users:disable", "nobody@example.test")

      expect(status).not_to eq(0)
      expect(err).to include("No user with email nobody@example.test")
    end
  end

  describe "users:enable" do
    it "sets active back to true, and the user can sign in again" do
      user = User.create!(email: "returning@example.test", role: "viewer", provider: User::PASSWORD_PROVIDER,
                          password: "returning-password-1234", active: false)

      status, out, err = run_task("users:enable", " Returning@Example.test ")

      expect(status).to eq(0), err
      expect(out).to include("Enabled returning@example.test")
      expect(user.reload.active).to be(true)
      expect(User.authenticate_password_login(email: "returning@example.test", password: "returning-password-1234"))
        .to eq(user)
    end

    it "succeeds and leaves an already-enabled user enabled, as users:disable does for a disabled one" do
      user = User.create!(email: "here@example.test", role: "admin", provider: User::PASSWORD_PROVIDER,
                          password: "here-password-1234")

      status, out, err = run_task("users:enable", "here@example.test")

      expect(status).to eq(0), err
      expect(out).to include("Enabled here@example.test")
      expect(user.reload).to have_attributes(active: true, role: "admin")
    end

    it "undoes users:disable" do
      user = User.create!(email: "flip@example.test", role: "viewer", provider: User::PASSWORD_PROVIDER,
                          password: "flip-password-1234")

      run_task("users:disable", "flip@example.test")
      expect(user.reload.active).to be(false)
      status, _out, err = run_task("users:enable", "flip@example.test")

      expect(status).to eq(0), err
      expect(user.reload.active).to be(true)
    end

    it "also works in oidc mode, for OIDC users" do
      use_oidc_mode
      user = User.create!(email: "sso@example.test", role: "viewer", provider: oidc_provider, provider_uid: "sub-sso",
                          active: false)

      status, _out, err = run_task("users:enable", "sso@example.test")

      expect(status).to eq(0), err
      expect(user.reload.active).to be(true)
    end

    it "fails clearly for an unknown email" do
      status, _out, err = run_task("users:enable", "nobody@example.test")

      expect(status).not_to eq(0)
      expect(err).to include("No user with email nobody@example.test")
      expect(User.count).to eq(0)
    end
  end

  describe "users:reset_password" do
    let!(:user) do
      User.create!(email: "forgetful@example.test", role: "viewer", provider: User::PASSWORD_PROVIDER,
                   password: "old-password-123456789")
    end

    it "prints a new password and retires the old one" do
      status, out, err = run_task("users:reset_password", "FORGETFUL@example.test")

      expect(status).to eq(0), err
      password = printed_password(out)
      expect(password.length).to be >= 20
      user.reload
      expect(user.authenticate(password)).to eq(user)
      expect(user.authenticate("old-password-123456789")).to be(false)
    end

    it "leaves a disabled user disabled" do
      user.update!(active: false)

      status, _out, err = run_task("users:reset_password", "forgetful@example.test")

      expect(status).to eq(0), err
      expect(user.reload.active).to be(false)
    end

    it "fails clearly for an unknown email" do
      status, out, err = run_task("users:reset_password", "nobody@example.test")

      expect(status).not_to eq(0)
      expect(err).to include("No user with email nobody@example.test")
      expect(printed_password(out)).to be_nil
    end

    it "refuses an OIDC user, who has no password" do
      sso = User.create!(email: "sso@example.test", role: "viewer", provider: oidc_provider, provider_uid: "sub-sso")

      status, out, err = run_task("users:reset_password", "sso@example.test")

      expect(status).not_to eq(0)
      expect(err).to include("doesn't sign in with a password")
      expect(printed_password(out)).to be_nil
      expect(sso.reload.password_digest).to be_nil
    end

    it "refuses to run in oidc mode" do
      use_oidc_mode

      status, out, err = run_task("users:reset_password", "forgetful@example.test")

      expect(status).not_to eq(0)
      expect(err).to include("ROTTEN_UI_AUTH=password")
      expect(printed_password(out)).to be_nil
      expect(user.reload.authenticate("old-password-123456789")).to eq(user)
    end
  end
end
