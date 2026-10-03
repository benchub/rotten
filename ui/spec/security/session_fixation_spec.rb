require "rails_helper"

# Every sign-in resets the session, so an ID or cookie an attacker planted
# before login is worthless afterwards, and sign-out drops the session.
RSpec.describe "Session fixation", type: :request do
  let(:cookie_name) { Rails.application.config.session_options.fetch(:key) }

  before { User.delete_all }

  # The page's global token, which is valid for any form in the session.
  def pre_login_token
    with_forgery_protection { get "/login" }
    Nokogiri::HTML(response.body).at_css("meta[name='csrf-token']")["content"]
  end

  # rspec-rails' redirect_to matcher always checks the example's own last
  # response, so a second session's response is checked by hand.
  def expect_sent_to_login(other_response)
    expect(other_response).to have_http_status(:found)
    expect(URI(other_response.location).path).to eq("/login")
  end

  def with_forgery_protection
    original = ActionController::Base.allow_forgery_protection
    ActionController::Base.allow_forgery_protection = true
    yield
  ensure
    ActionController::Base.allow_forgery_protection = original
  end

  shared_examples "a login that starts a fresh session" do
    it "changes the session ID" do
      with_forgery_protection { get "/login" }
      planted_id = session.id.to_s
      expect(planted_id).to be_present

      log_in

      expect(session[:user_id]).to eq(user.id)
      expect(session.id.to_s).not_to eq(planted_id)
    end

    it "keeps none of the pre-login session's data" do
      with_forgery_protection { get "/login" }
      pre_login_token = session[:_csrf_token]
      expect(pre_login_token).to be_present

      log_in

      expect(session[:_csrf_token]).not_to eq(pre_login_token)
    end

    it "doesn't let a session cookie planted before login act as the signed-in session" do
      with_forgery_protection { get "/login" }
      planted = cookies[cookie_name]
      expect(planted).to be_present

      log_in
      expect(cookies[cookie_name]).not_to eq(planted)

      attacker = open_session
      attacker.cookies[cookie_name] = planted
      attacker.get "/"
      expect_sent_to_login(attacker.response)
    end

    it "refuses the pre-login authenticity token afterwards" do
      token = pre_login_token

      log_in
      with_forgery_protection { delete "/logout", params: { authenticity_token: token } }

      expect(response).to have_http_status(:unprocessable_content)
      get "/"
      expect(response).to have_http_status(:ok)
    end

    it "ends the session on logout" do
      log_in
      signed_in_id = session.id.to_s

      delete "/logout"

      expect(session[:user_id]).to be_nil
      expect(session.id.to_s).not_to eq(signed_in_id)
      get "/"
      expect(response).to redirect_to("/login")
    end

    # The session lives in the cookie (CookieStore), so the server has nothing
    # to delete: a copy of the cookie taken before logout still works until
    # the secret rotates. Needs a server-side session store or a per-user
    # session generation; see the follow-up in the task report.
    it "refuses a copy of the session cookie after logout" do
      pending "CookieStore sessions can't be revoked server-side"
      log_in
      stolen = cookies[cookie_name]

      delete "/logout"

      thief = open_session
      thief.cookies[cookie_name] = stolen
      thief.get "/"
      expect_sent_to_login(thief.response)
    end
  end

  context "with a password login" do
    let(:password) { "fixation-spec-password-123" }
    let!(:user) { create_password_user(email: "fixation@example.test", password: password) }

    def log_in
      password_sign_in(email: user.email, password: password)
      expect(response).to redirect_to("/")
    end

    it_behaves_like "a login that starts a fresh session"
  end

  context "with an OIDC login" do
    let(:user) { User.find_by!(provider_uid: "sub-fixation") }

    before do
      use_oidc_mode(viewer_group: "rotten-viewers")
      mock_oidc_auth(uid: "sub-fixation", email: "fixation@example.test", groups: ["rotten-viewers"])
    end

    def log_in
      oidc_sign_in
      expect(response).to redirect_to("/")
    end

    it_behaves_like "a login that starts a fresh session"
  end
end
