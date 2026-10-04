require "rails_helper"

RSpec.describe "Changing your own password", type: :request do
  include ActiveSupport::Testing::TimeHelpers

  let(:password) { "current-password-1234" }
  let(:new_password) { "a-brand-new-password-5678" }

  before do
    User.delete_all
  end

  let!(:user) { create_password_user(email: "changer@example.test", password: password) }

  def alert_text
    Nokogiri::HTML(response.body).at_css("[role='alert']")&.text.to_s.strip
  end

  def change_password(current: password, new: new_password, confirmation: new, ip: "127.0.0.1")
    patch "/password", params: { current_password: current, password: new, password_confirmation: confirmation },
                       env: { "REMOTE_ADDR" => ip }
  end

  # rspec-rails' redirect_to matcher checks the example's own last response.
  def expect_sent_to_login(other_response)
    expect(other_response).to have_http_status(:found)
    expect(URI(other_response.location).path).to eq("/login")
  end

  def expect_password_unchanged
    expect(user.reload.authenticate(password)).to eq(user)
    expect(user.authenticate(new_password)).to be(false)
  end

  describe "the page" do
    it "shows a password user a form with current, new and confirmation fields" do
      password_sign_in(email: user.email, password: password)

      get "/password"

      expect(response).to have_http_status(:ok)
      form = Nokogiri::HTML(response.body).at_css("form[action='/password']")
      expect(form).to be_present
      expect(form.at_css("input[name='_method'][value='patch']")).to be_present
      expect(form.at_css("input[type='password'][name='current_password'][autocomplete='current-password']"))
        .to be_present
      expect(form.at_css("input[type='password'][name='password'][autocomplete='new-password']")).to be_present
      expect(form.at_css("input[type='password'][name='password_confirmation'][autocomplete='new-password']"))
        .to be_present
    end

    it "is linked from the home page for a password user" do
      password_sign_in(email: user.email, password: password)

      get "/"

      expect(Nokogiri::HTML(response.body).at_css("a[href='/password']")).to be_present
    end

    it "sends a signed-out visitor to the login page" do
      get "/password"
      expect(response).to redirect_to("/login")

      change_password
      expect(response).to redirect_to("/login")
      expect_password_unchanged
    end
  end

  describe "a successful change" do
    it "changes the password, keeps this session signed in, and rotates the session ID" do
      password_sign_in(email: user.email, password: password)
      before_id = session.id.to_s
      old_credential = session[:credential]

      change_password

      expect(response).to redirect_to("/")
      expect(session[:user_id]).to eq(user.id)
      expect(session.id.to_s).not_to eq(before_id)
      expect(session[:credential]).not_to eq(old_credential)
      expect(session[:credential]).to eq(user.reload.credential_fingerprint)
      expect(user.authenticate(new_password)).to eq(user)
      expect(user.authenticate(password)).to be(false)

      follow_redirect!
      expect(response).to have_http_status(:ok)
      expect(response.body).to include("Password changed")
      get "/"
      expect(response).to have_http_status(:ok)
      expect(response.body).to include(user.email)
    end

    it "drops the user's other sessions on their next request" do
      password_sign_in(email: user.email, password: password)
      other = open_session
      other.post "/login", params: { email: user.email, password: password }
      other.get "/"
      expect(other.response).to have_http_status(:ok)

      change_password
      expect(response).to redirect_to("/")

      other.get "/"
      expect_sent_to_login(other.response)
      get "/"
      expect(response).to have_http_status(:ok)
    end

    it "lets the new password sign in, and not the old one" do
      password_sign_in(email: user.email, password: password)
      change_password

      password_sign_in(email: user.email, password: password)
      expect(response).to have_http_status(:unprocessable_content)
      password_sign_in(email: user.email, password: new_password)
      expect(response).to redirect_to("/")
    end
  end

  describe "a refused change" do
    it "refuses a wrong current password with a generic message, and keeps the session" do
      password_sign_in(email: user.email, password: password)

      change_password(current: "#{password}x")

      expect(response).to have_http_status(:unprocessable_content)
      expect(alert_text).to eq(PasswordsController::WRONG_CURRENT_PASSWORD)
      expect_password_unchanged
      expect(session[:user_id]).to eq(user.id)
      get "/"
      expect(response).to have_http_status(:ok)
    end

    [
      ["a missing current password", { password: "a-brand-new-password-5678",
                                        password_confirmation: "a-brand-new-password-5678" }],
      ["an array current password", { current_password: ["current-password-1234"],
                                       password: "a-brand-new-password-5678",
                                       password_confirmation: "a-brand-new-password-5678" }],
      ["a hash current password", { current_password: { "a" => "b" }, password: "a-brand-new-password-5678",
                                    password_confirmation: "a-brand-new-password-5678" }],
      ["an overlong current password", { current_password: "a" * 1000, password: "a-brand-new-password-5678",
                                         password_confirmation: "a-brand-new-password-5678" }]
    ].each do |label, params|
      it "refuses #{label} with the same generic message, not an error" do
        password_sign_in(email: user.email, password: password)

        patch "/password", params: params

        expect(response).to have_http_status(:unprocessable_content)
        expect(alert_text).to eq(PasswordsController::WRONG_CURRENT_PASSWORD)
        expect_password_unchanged
      end
    end

    it "runs exactly one bcrypt hash for a wrong current password, and changes nothing" do
      password_sign_in(email: user.email, password: password)
      allow(BCrypt::Engine).to receive(:hash_secret).and_call_original

      change_password(current: "wrong-password-0000")

      expect(BCrypt::Engine).to have_received(:hash_secret).once
      expect_password_unchanged
    end

    it "checks the current password before saying anything about the new one" do
      password_sign_in(email: user.email, password: password)

      change_password(current: "wrong-password-0000", new: "short", confirmation: "different")

      expect(alert_text).to eq(PasswordsController::WRONG_CURRENT_PASSWORD)
      expect(response.body).not_to match(/too short|doesn't match/i)
    end

    it "applies the user model's policy: a too-short new password" do
      password_sign_in(email: user.email, password: password)

      change_password(new: "short")

      expect(response).to have_http_status(:unprocessable_content)
      expect(response.body).to include("too short")
      expect(user.reload.authenticate(password)).to eq(user)
      expect(session[:user_id]).to eq(user.id)
    end

    it "refuses a confirmation that doesn't match" do
      password_sign_in(email: user.email, password: password)

      change_password(confirmation: "#{new_password}x")

      expect(response).to have_http_status(:unprocessable_content)
      expect(response.body).to include("doesn&#39;t match")
      expect_password_unchanged
    end

    it "refuses a blank new password" do
      password_sign_in(email: user.email, password: password)

      change_password(new: "", confirmation: "")

      expect(response).to have_http_status(:unprocessable_content)
      expect(user.reload.authenticate(password)).to eq(user)
    end

    it "never echoes any of the passwords back into the form" do
      password_sign_in(email: user.email, password: password)

      change_password(current: "typed-current-0000", new: "typed-new-0000", confirmation: "typed-confirm-0000")

      expect(response.body).not_to include("typed-current-0000")
      expect(response.body).not_to include("typed-new-0000")
      expect(response.body).not_to include("typed-confirm-0000")
    end

    it "refuses a disabled user, whose session is dropped first" do
      password_sign_in(email: user.email, password: password)
      user.update!(active: false)

      change_password

      expect(response).to redirect_to("/login")
      expect_password_unchanged
    end
  end

  describe "OIDC users" do
    let(:oidc_user) do
      User.create!(email: "sso@example.test", role: "admin", provider: oidc_provider, provider_uid: "sub-sso")
    end

    it "get a 404 for the page and the update, and no link" do
      post "/__test/sign_in", params: { user_id: oidc_user.id }

      get "/"
      expect(response).to have_http_status(:ok)
      expect(Nokogiri::HTML(response.body).at_css("a[href='/password']")).to be_nil

      get "/password"
      expect(response).to have_http_status(:not_found)

      patch "/password", params: { current_password: "", password: new_password, password_confirmation: new_password }
      expect(response).to have_http_status(:not_found)
      expect(oidc_user.reload.password_digest).to be_nil
    end

    it "get a 404 even if the row somehow has a digest" do
      oidc_user.update!(password: password)
      post "/__test/sign_in", params: { user_id: oidc_user.id }

      change_password

      expect(response).to have_http_status(:not_found)
      expect(oidc_user.reload.authenticate(password)).to eq(oidc_user)
    end

    it "get a 404 in oidc mode, signed in through OIDC" do
      use_oidc_mode(viewer_group: "rotten-viewers")
      mock_oidc_auth(uid: "sub-1", email: "person@example.test", groups: ["rotten-viewers"])
      oidc_sign_in

      get "/password"
      expect(response).to have_http_status(:not_found)
      change_password
      expect(response).to have_http_status(:not_found)
    end
  end

  describe "rate limiting" do
    it "limits current-password checks per user, across IP addresses" do
      password_sign_in(email: user.email, password: password)

      PasswordsController::ATTEMPTS_PER_USER.times do |i|
        change_password(current: "wrong-#{i}", ip: "10.4.0.#{i + 1}")
        expect(response).to have_http_status(:unprocessable_content)
      end

      change_password(ip: "10.5.0.1")

      expect(response).to have_http_status(:too_many_requests)
      expect(alert_text).to eq(PasswordsController::TOO_MANY_ATTEMPTS)
      expect_password_unchanged
      expect(session[:user_id]).to eq(user.id)
    end

    it "limits attempts from one IP address across users" do
      users = Array.new(3) do |i|
        create_password_user(email: "user#{i}@example.test", password: password)
      end
      sessions = users.map do |u|
        open_session.tap { |s| s.post "/login", params: { email: u.email, password: password } }
      end

      PasswordsController::ATTEMPTS_PER_IP.times do |i|
        s = sessions[i % sessions.size]
        s.patch "/password", params: { current_password: "wrong", password: new_password,
                                       password_confirmation: new_password }, env: { "REMOTE_ADDR" => "10.6.0.1" }
        expect(s.response).to have_http_status(:unprocessable_content)
      end

      password_sign_in(email: user.email, password: password)
      change_password(ip: "10.6.0.1")
      expect(response).to have_http_status(:too_many_requests)
      expect_password_unchanged

      change_password(ip: "10.6.0.2")
      expect(response).to redirect_to("/")
    end

    it "counts separately from login attempts" do
      SessionsController::ATTEMPTS_PER_IP.times do |i|
        password_sign_in(email: "nobody#{i}@example.test", password: "wrong", ip: "10.7.0.1")
      end
      password_sign_in(email: user.email, password: password, ip: "10.7.0.2")

      change_password(ip: "10.7.0.1")

      expect(response).to redirect_to("/")
    end

    it "lets attempts through again once the window has passed" do
      password_sign_in(email: user.email, password: password)
      PasswordsController::ATTEMPTS_PER_USER.times { |i| change_password(current: "wrong-#{i}") }
      change_password
      expect(response).to have_http_status(:too_many_requests)

      travel(PasswordsController::ATTEMPTS_WINDOW + 1.second) do
        change_password
      end

      expect(response).to redirect_to("/")
    end
  end
end
