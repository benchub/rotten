require "rails_helper"
require "rake"

# Sessions live in the encrypted cookie, so the server can't delete one.
# Each session stores users.session_generation from when it started, and
# every bump ends all of that user's sessions, including a copy of a cookie.
RSpec.describe "Session revocation", type: :request do
  let(:cookie_name) { Rails.application.config.session_options.fetch(:key) }

  before { User.delete_all }

  def expect_sent_to_login(other_response)
    expect(other_response).to have_http_status(:found)
    expect(URI(other_response.location).path).to eq("/login")
  end

  def replay(cookie)
    client = open_session
    client.cookies[cookie_name] = cookie
    client.get "/"
    client.response
  end

  # Signs in, checks the cookie works from another client, and returns it.
  def copied_cookie
    sign_in
    get "/"
    expect(response).to have_http_status(:ok)
    copied = cookies[cookie_name]
    expect(copied).to be_present
    expect(replay(copied)).to have_http_status(:ok)
    copied
  end

  def run_task(name, *args)
    Rails.application.load_tasks unless Rake::Task.task_defined?(name)
    task = Rake::Task[name]
    task.reenable
    original = $stdout
    $stdout = StringIO.new
    task.invoke(*args)
  ensure
    $stdout = original
  end

  shared_examples "revoked by logout" do
    it "ends every session the user has, not just the one that logged out" do
      copied = copied_cookie

      other = open_session
      other.cookies[cookie_name] = copied
      other.delete "/logout"
      expect_sent_to_login(other.response)

      expect_sent_to_login(replay(copied))
      get "/"
      expect(response).to redirect_to("/login")
    end

    it "bumps session_generation" do
      sign_in

      expect { delete "/logout" }.to change { user.reload.session_generation }.by(1)
    end
  end

  shared_examples "revoked by users:disable" do
    it "refuses a copy of the cookie, and bumps session_generation" do
      copied = copied_cookie

      expect { run_task("users:disable", user.email) }.to change { user.reload.session_generation }.by(1)

      expect_sent_to_login(replay(copied))
    end
  end

  context "for a password user" do
    let(:password) { "revocation-spec-password-1" }
    let!(:user) { create_password_user(email: "revoked@example.test", password: password) }

    def sign_in
      password_sign_in(email: user.email, password: password)
      expect(response).to redirect_to("/")
    end

    it_behaves_like "revoked by logout"
    it_behaves_like "revoked by users:disable"

    it "refuses a copy of the cookie after users:reset_password, and bumps session_generation" do
      copied = copied_cookie

      expect { run_task("users:reset_password", user.email) }.to change { user.reload.session_generation }.by(1)

      expect_sent_to_login(replay(copied))
    end

    it "refuses a copy of the cookie after a password change, and keeps the changing session signed in" do
      copied = copied_cookie
      new_password = "revocation-spec-password-2"

      expect do
        patch "/password", params: { current_password: password, password: new_password,
                                     password_confirmation: new_password }
      end.to change { user.reload.session_generation }.by(1)
      expect(response).to redirect_to("/")

      expect_sent_to_login(replay(copied))
      get "/"
      expect(response).to have_http_status(:ok)
      expect(session[:session_generation]).to eq(user.reload.session_generation)
    end

    it "refuses a session whose generation is behind the user's, even with the right password fingerprint" do
      copied = copied_cookie

      user.revoke_sessions!

      expect_sent_to_login(replay(copied))
    end

    it "doesn't bump session_generation on a failed login" do
      sign_in

      expect { password_sign_in(email: user.email, password: "wrong-#{password}") }
        .not_to(change { user.reload.session_generation })
    end

    it "checks the generation without another query: one users lookup per request" do
      sign_in
      user_queries = []
      callback = lambda do |*, payload|
        user_queries << payload[:sql] if payload[:sql].match?(/\bFROM\s+"users"/i)
      end

      ActiveSupport::Notifications.subscribed(callback, "sql.active_record") { get "/" }

      expect(response).to have_http_status(:ok)
      expect(user_queries.size).to eq(1), user_queries.join("\n")
    end
  end

  context "for an OIDC user" do
    let(:user) { User.find_by!(provider_uid: "sub-revoked") }

    before do
      use_oidc_mode(viewer_group: "rotten-viewers")
      mock_oidc_auth(uid: "sub-revoked", email: "revoked@example.test", groups: ["rotten-viewers"])
    end

    def sign_in
      oidc_sign_in
      expect(response).to redirect_to("/")
    end

    it_behaves_like "revoked by logout"
    it_behaves_like "revoked by users:disable"

    # Group membership is only checked at login, so a login refused because
    # the user left the groups ends the sessions they already have.
    it "ends the user's other sessions when a login is refused for lost group access" do
      copied = copied_cookie

      mock_oidc_auth(uid: "sub-revoked", email: "revoked@example.test", groups: ["someone-else"])
      other = open_session
      other.post "/auth/openid_connect"
      expect { other.get "/auth/openid_connect/callback" }.to change { user.reload.session_generation }.by(1)
      expect(other.response).to have_http_status(:forbidden)

      expect_sent_to_login(replay(copied))

      # Getting the group back allows a new login, but not the old session.
      mock_oidc_auth(uid: "sub-revoked", email: "revoked@example.test", groups: ["rotten-viewers"])
      sign_in
      get "/"
      expect(response).to have_http_status(:ok)
      expect_sent_to_login(replay(copied))
    end

    # The resync's email update hits users.email unique and rolls back; the
    # bump must not ride along in that rolled-back transaction.
    it "ends the user's other sessions when lost group access comes with an email another user has" do
      copied = copied_cookie
      User.create!(email: "taken@example.test", role: "viewer")

      mock_oidc_auth(uid: "sub-revoked", email: "taken@example.test", groups: ["someone-else"])
      other = open_session
      other.post "/auth/openid_connect"
      expect { other.get "/auth/openid_connect/callback" }.to change { user.reload.session_generation }.by(1)
      expect(other.response).to have_http_status(:forbidden)
      expect(other.session[:user_id]).to be_nil
      expect(user.reload.email).to eq("revoked@example.test")

      expect_sent_to_login(replay(copied))
    end

    it "doesn't touch session_generation when a login for an unknown user is refused" do
      sign_in
      mock_oidc_auth(uid: "sub-stranger", email: "stranger@example.test", groups: ["someone-else"])

      expect { oidc_sign_in }.not_to(change { user.reload.session_generation })
      expect(response).to have_http_status(:forbidden)
    end
  end
end
