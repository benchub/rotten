require "rails_helper"

# Changing your password ends every other session, including a copy of this
# session's cookie taken before the change, while the session that made the
# change carries on under a new session ID and credential fingerprint.
RSpec.describe "Password change sessions", type: :request do
  let(:cookie_name) { Rails.application.config.session_options.fetch(:key) }
  let(:password) { "security-change-password-1" }
  let(:new_password) { "security-change-password-2" }

  before { User.delete_all }

  let!(:user) { create_password_user(email: "changer@example.test", password: password) }

  def expect_sent_to_login(other_response)
    expect(other_response).to have_http_status(:found)
    expect(URI(other_response.location).path).to eq("/login")
  end

  def change_password
    patch "/password", params: { current_password: password, password: new_password,
                                 password_confirmation: new_password }
    expect(response).to redirect_to("/")
  end

  it "refuses a copy of the session cookie taken before the change" do
    password_sign_in(email: user.email, password: password)
    get "/"
    stolen = cookies[cookie_name]
    expect(stolen).to be_present

    thief = open_session
    thief.cookies[cookie_name] = stolen
    thief.get "/"
    expect(thief.response).to have_http_status(:ok)

    change_password
    expect(cookies[cookie_name]).not_to eq(stolen)

    replay = open_session
    replay.cookies[cookie_name] = stolen
    replay.get "/"
    expect_sent_to_login(replay.response)
    thief.get "/"
    expect_sent_to_login(thief.response)

    get "/"
    expect(response).to have_http_status(:ok)
    expect(response.body).to include(user.email)
  end

  it "keeps the new session's cookie working on its own, from another client" do
    password_sign_in(email: user.email, password: password)
    change_password
    fresh = cookies[cookie_name]

    other = open_session
    other.cookies[cookie_name] = fresh
    other.get "/"

    expect(other.response).to have_http_status(:ok)
    expect(other.response.body).to include(user.email)
  end

  it "keeps neither the password nor its digest in the new session" do
    password_sign_in(email: user.email, password: password)
    change_password

    values = session.to_hash.values.map(&:to_s).join
    expect(values).not_to include(new_password)
    expect(values).not_to include(user.reload.password_digest)
  end

  it "doesn't write any of the passwords to the request log" do
    password_sign_in(email: user.email, password: password)
    log = StringIO.new
    original = ActionController::Base.logger
    ActionController::Base.logger = ActiveSupport::Logger.new(log)
    begin
      patch "/password", params: { current_password: "wrong-#{password}", password: new_password,
                                   password_confirmation: new_password }
      change_password
    ensure
      ActionController::Base.logger = original
    end

    expect(log.string).to include("PasswordsController#update")
    expect(log.string).not_to include(password)
    expect(log.string).not_to include(new_password)
  end
end
