require "rails_helper"

# users:disable ends a user's sessions on their next request, but sessions
# live in the cookie and never expire, so a copy taken before the disable
# still names the user. Once users:enable runs, that copy works again.
# 20261003-150000-1 adds users.session_generation, bumped by disable, which
# revokes it; until then these examples are pending.
RSpec.describe "Sessions across users:disable and users:enable", type: :request do
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

  shared_examples "a re-enabled user's old session" do
    it "is refused after users:disable and then users:enable" do
      sign_in
      get "/"
      expect(response).to have_http_status(:ok)
      copied = cookies[cookie_name]
      expect(copied).to be_present
      expect(replay(copied)).to have_http_status(:ok)

      UserAdmin.disable(user.email)
      expect_sent_to_login(replay(copied))

      UserAdmin.enable(user.email)
      expect(user.reload).to be_active

      pending "revoked by session_generation in 20261003-150000-1"
      expect_sent_to_login(replay(copied))
    end
  end

  context "for a password user" do
    let(:password) { "reenable-spec-password-1" }
    let!(:user) { create_password_user(email: "reenabled@example.test", password: password) }

    def sign_in
      password_sign_in(email: user.email, password: password)
      expect(response).to redirect_to("/")
    end

    it_behaves_like "a re-enabled user's old session"
  end

  context "for an OIDC user" do
    let(:user) { User.find_by!(provider_uid: "sub-reenabled") }

    before do
      use_oidc_mode(viewer_group: "rotten-viewers")
      mock_oidc_auth(uid: "sub-reenabled", email: "reenabled@example.test", groups: ["rotten-viewers"])
    end

    def sign_in
      oidc_sign_in
      expect(response).to redirect_to("/")
    end

    it_behaves_like "a re-enabled user's old session"
  end
end
