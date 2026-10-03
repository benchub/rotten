require "rails_helper"

RSpec.describe "Password login", type: :request do
  include ActiveSupport::Testing::TimeHelpers

  let(:password) { "correct-horse-battery-staple" }
  let(:generic_failure) { SessionsController::FAILURE }

  before do
    User.delete_all
  end

  def alert_text
    Nokogiri::HTML(response.body).at_css("[role='alert']")&.text.to_s.strip
  end

  describe "the login page" do
    it "shows an email and password form that posts to /login" do
      get "/login"

      form = Nokogiri::HTML(response.body).at_css("form[action='/login']")
      expect(form).to be_present
      expect(form["method"]).to eq("post")
      expect(form.at_css("input[type='email'][name='email']")).to be_present
      expect(form.at_css("input[type='password'][name='password']")).to be_present
    end
  end

  describe "signing in" do
    it "signs in with the right password, resets the session and records the login" do
      user = create_password_user(email: "viewer@example.test", password: password)
      post "/__test/sign_in", params: { user_id: user.id }
      before_id = session.id.to_s

      password_sign_in(email: "viewer@example.test", password: password)

      expect(response).to redirect_to("/")
      expect(session[:user_id]).to eq(user.id)
      expect(session.id.to_s).not_to eq(before_id)
      expect(user.reload.last_login_at).to be_within(1.minute).of(Time.current)

      get "/"
      expect(response).to have_http_status(:ok)
      expect(response.body).to include("viewer@example.test")
    end

    it "matches the email regardless of case and surrounding spaces" do
      user = create_password_user(email: "mixed@example.test", password: password)

      password_sign_in(email: "  Mixed@Example.TEST ", password: password)

      expect(session[:user_id]).to eq(user.id)
    end

    it "gives an admin access to admin pages" do
      create_password_user(email: "admin@example.test", password: password, role: "admin")

      password_sign_in(email: "admin@example.test", password: password)
      get "/admin"

      expect(response).to have_http_status(:ok)
    end

    it "refuses a wrong password with a generic message and no session" do
      user = create_password_user(email: "viewer@example.test", password: password)

      password_sign_in(email: "viewer@example.test", password: "#{password}x")

      expect(response).to have_http_status(:unprocessable_content)
      expect(alert_text).to eq(generic_failure)
      expect(session[:user_id]).to be_nil
      expect(user.reload.last_login_at).to be_nil
    end

    it "gives an unknown email exactly the same response as a wrong password" do
      create_password_user(email: "known@example.test", password: password)

      password_sign_in(email: "known@example.test", password: "wrong-password")
      wrong_password = [response.status, alert_text, response.body.gsub("known@example.test", "EMAIL")]
      password_sign_in(email: "unknown@example.test", password: "wrong-password")
      unknown_email = [response.status, alert_text, response.body.gsub("unknown@example.test", "EMAIL")]

      expect(unknown_email).to eq(wrong_password)
      expect(unknown_email.first(2)).to eq([422, generic_failure])
      expect(session[:user_id]).to be_nil
    end

    it "refuses a disabled user even with the right password, with the same generic message" do
      user = create_password_user(email: "disabled@example.test", password: password, active: false)

      password_sign_in(email: "disabled@example.test", password: password)

      expect(response).to have_http_status(:unprocessable_content)
      expect(alert_text).to eq(generic_failure)
      expect(body_text).not_to match(/disabled/i)
      expect(session[:user_id]).to be_nil
      expect(user.reload.last_login_at).to be_nil
    end

    it "never signs in an OIDC user by password" do
      oidc_user = User.create!(email: "oidc@example.test", role: "admin", provider: oidc_provider, provider_uid: "sub-1")

      password_sign_in(email: "oidc@example.test", password: "")
      expect(session[:user_id]).to be_nil
      password_sign_in(email: "oidc@example.test", password: "anything")
      expect(session[:user_id]).to be_nil

      # Even with a digest on the row, only provider "password" may use it.
      oidc_user.update!(password: password)
      password_sign_in(email: "oidc@example.test", password: password)

      expect(response).to have_http_status(:unprocessable_content)
      expect(alert_text).to eq(generic_failure)
      expect(session[:user_id]).to be_nil
    end

    it "refuses a user with no provider, even with a digest" do
      User.create!(email: "legacy@example.test", role: "viewer", password: password)

      password_sign_in(email: "legacy@example.test", password: password)

      expect(response).to have_http_status(:unprocessable_content)
      expect(session[:user_id]).to be_nil
    end

    # Every failure costs one bcrypt hash, so response time doesn't reveal
    # which emails exist, belong to OIDC users, or are disabled.
    {
      "a wrong password" => ["known@example.test", :wrong],
      "an unknown email" => ["unknown@example.test", :wrong],
      "an OIDC user's email" => ["oidc@example.test", :wrong],
      "a disabled user with the right password" => ["off@example.test", :right]
    }.each do |label, (email, which)|
      it "runs exactly one bcrypt hash for #{label}" do
        create_password_user(email: "known@example.test", password: password)
        create_password_user(email: "off@example.test", password: password, active: false)
        User.create!(email: "oidc@example.test", role: "viewer", provider: oidc_provider, provider_uid: "sub-2")
        allow(BCrypt::Engine).to receive(:hash_secret).and_call_original

        password_sign_in(email: email, password: which == :right ? password : "wrong-password")

        expect(BCrypt::Engine).to have_received(:hash_secret).once
        expect(alert_text).to eq(generic_failure)
      end
    end

    [
      ["a missing email", { password: "secret" }],
      ["a missing password", { email: "viewer@example.test" }],
      ["an array email", { email: ["viewer@example.test"], password: "secret" }],
      ["a hash password", { email: "viewer@example.test", password: { "a" => "b" } }]
    ].each do |label, params|
      it "refuses #{label} with the generic message, not an error" do
        create_password_user(email: "viewer@example.test", password: password)

        post "/login", params: params

        expect(response).to have_http_status(:unprocessable_content)
        expect(alert_text).to eq(generic_failure)
        expect(session[:user_id]).to be_nil
      end
    end

    it "ends an existing session when a later login fails" do
      user = create_password_user(email: "viewer@example.test", password: password)
      password_sign_in(email: "viewer@example.test", password: password)
      expect(session[:user_id]).to eq(user.id)

      password_sign_in(email: "viewer@example.test", password: "wrong-password")

      expect(session[:user_id]).to be_nil
    end

    it "keeps the email in the form after a failure, but never the password" do
      password_sign_in(email: "typo@example.test", password: "secret-attempt")

      form = Nokogiri::HTML(response.body).at_css("form[action='/login']")
      expect(form.at_css("input[name='email']")["value"]).to eq("typo@example.test")
      expect(form.at_css("input[name='password']")["value"]).to be_nil
      expect(response.body).not_to include("secret-attempt")
    end
  end

  describe "rate limiting" do
    it "limits attempts from one IP address" do
      create_password_user(email: "target@example.test", password: password)

      SessionsController::ATTEMPTS_PER_IP.times do |i|
        password_sign_in(email: "nobody#{i}@example.test", password: "wrong", ip: "10.0.0.1")
        expect(response).to have_http_status(:unprocessable_content)
      end

      password_sign_in(email: "target@example.test", password: password, ip: "10.0.0.1")
      expect(response).to have_http_status(:too_many_requests)
      expect(alert_text).to eq(SessionsController::TOO_MANY_ATTEMPTS)
      expect(session[:user_id]).to be_nil

      password_sign_in(email: "target@example.test", password: password, ip: "10.0.0.2")
      expect(response).to redirect_to("/")
    end

    it "limits attempts against one email from many IP addresses" do
      create_password_user(email: "target@example.test", password: password)
      create_password_user(email: "bystander@example.test", password: password)

      SessionsController::ATTEMPTS_PER_EMAIL.times do |i|
        email = i.even? ? "target@example.test" : " TARGET@example.test"
        password_sign_in(email: email, password: "wrong", ip: "10.1.0.#{i + 1}")
        expect(response).to have_http_status(:unprocessable_content)
      end

      password_sign_in(email: "Target@Example.test", password: password, ip: "10.2.0.1")
      expect(response).to have_http_status(:too_many_requests)
      expect(session[:user_id]).to be_nil

      password_sign_in(email: "bystander@example.test", password: password, ip: "10.2.0.1")
      expect(response).to redirect_to("/")
    end

    it "lets attempts through again once the window has passed" do
      create_password_user(email: "target@example.test", password: password)
      SessionsController::ATTEMPTS_PER_IP.times do |i|
        password_sign_in(email: "nobody#{i}@example.test", password: "wrong", ip: "10.3.0.1")
      end
      password_sign_in(email: "target@example.test", password: password, ip: "10.3.0.1")
      expect(response).to have_http_status(:too_many_requests)

      travel(SessionsController::ATTEMPTS_WINDOW + 1.second) do
        password_sign_in(email: "target@example.test", password: password, ip: "10.3.0.1")
      end

      expect(response).to redirect_to("/")
    end
  end

  describe "session revocation on password change" do
    it "drops an existing session on its next request after users:reset_password" do
      user = create_password_user(email: "viewer@example.test", password: password)
      password_sign_in(email: "viewer@example.test", password: password)
      get "/"
      expect(response).to have_http_status(:ok)

      UserAdmin.reset_password("viewer@example.test")
      get "/"

      expect(response).to redirect_to("/login")
      expect(session[:user_id]).to be_nil
      expect(user.reload).to be_active
    end

    it "drops the stale session even on a login page request" do
      user = create_password_user(email: "viewer@example.test", password: password)
      password_sign_in(email: "viewer@example.test", password: password)

      user.update!(password: "a-completely-different-password")
      get "/login"

      expect(response).to have_http_status(:ok)
      expect(session[:user_id]).to be_nil
    end

    it "lets the new password start a fresh session after a reset" do
      create_password_user(email: "viewer@example.test", password: password)
      password_sign_in(email: "viewer@example.test", password: password)
      new_password = UserAdmin.reset_password("viewer@example.test").password

      password_sign_in(email: "viewer@example.test", password: new_password)
      get "/"

      expect(response).to have_http_status(:ok)
    end

    it "keeps a normal session alive across requests and unrelated user changes" do
      user = create_password_user(email: "viewer@example.test", password: password)
      password_sign_in(email: "viewer@example.test", password: password)

      get "/"
      expect(response).to have_http_status(:ok)
      user.update!(name: "Renamed", last_login_at: Time.current)
      get "/"
      expect(response).to have_http_status(:ok)
      get "/"

      expect(response).to have_http_status(:ok)
      expect(session[:user_id]).to eq(user.id)
    end

    it "doesn't keep the password digest in the session" do
      user = create_password_user(email: "viewer@example.test", password: password)
      password_sign_in(email: "viewer@example.test", password: password)

      values = session.to_hash.values.map(&:to_s)
      expect(values).not_to include(user.password_digest)
      expect(values.join).not_to include(user.password_digest)
    end

    it "leaves an OIDC session alone" do
      use_oidc_mode(viewer_group: "rotten-viewers")
      mock_oidc_auth(uid: "sub-1", email: "person@example.test", groups: ["rotten-viewers"])
      oidc_sign_in
      user = User.sole
      expect(user.password_digest).to be_nil

      get "/"
      expect(response).to have_http_status(:ok)
      user.update!(name: "Renamed")
      get "/"

      expect(response).to have_http_status(:ok)
      expect(session[:user_id]).to eq(user.id)
    end
  end

  context "in oidc mode" do
    before { use_oidc_mode }

    it "returns 404 for the password endpoint, even with the right password" do
      create_password_user(email: "viewer@example.test", password: password)

      password_sign_in(email: "viewer@example.test", password: password)

      expect(response).to have_http_status(:not_found)
      expect(session[:user_id]).to be_nil
    end

    it "doesn't show the password form" do
      get "/login"

      expect(response.body).not_to include("type=\"password\"")
      expect(Nokogiri::HTML(response.body).at_css("form[action='/login']")).to be_nil
    end
  end

  context "in password mode" do
    it "returns 404 for the OIDC failure endpoint" do
      get "/auth/failure"

      expect(response).to have_http_status(:not_found)
    end
  end
end
