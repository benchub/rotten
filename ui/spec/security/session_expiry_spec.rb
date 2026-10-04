require "rails_helper"

# Every session gets an absolute expiry at login, 12 hours by default, that
# activity doesn't extend. Past it, the session is reset and the user has to
# sign in again, which for OIDC users re-checks their groups. Sessions with no
# expiry or generation, from before either existed, count as expired.
RSpec.describe "Session expiry", type: :request do
  let(:cookie_name) { Rails.application.config.session_options.fetch(:key) }
  let(:password) { "expiry-spec-password-1" }

  before { User.delete_all }

  let!(:user) { create_password_user(email: "expiring@example.test", password: password) }

  around do |example|
    lifetime = Rails.configuration.x.session_lifetime_seconds
    example.run
  ensure
    Rails.configuration.x.session_lifetime_seconds = lifetime
  end

  def sign_in
    password_sign_in(email: user.email, password: password)
    expect(response).to redirect_to("/")
  end

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

  # An encrypted session cookie holding exactly data, as the app's own cookie
  # store would write it.
  def session_cookie(data)
    jar = ActionDispatch::TestRequest.create(Rails.application.env_config.dup).cookie_jar
    jar.encrypted[cookie_name] = { value: data.merge("session_id" => SecureRandom.hex(16)) }
    jar[cookie_name]
  end

  it "defaults to 12 hours" do
    expect(Rails.configuration.x.session_lifetime_seconds).to eq(12.hours.to_i)
  end

  it "stamps an absolute expiry, as an integer epoch, at login" do
    freeze_time
    sign_in

    expect(session[:expires_at]).to eq((Time.current + 12.hours).to_i)
    expect(session[:session_generation]).to eq(user.reload.session_generation)
  end

  it "works until the lifetime ends, then sends the user to log in again with a message" do
    start = Time.current.change(usec: 0)
    travel_to(start) { sign_in }

    travel_to(start + 12.hours - 1.second) do
      get "/"
      expect(response).to have_http_status(:ok)
    end

    travel_to(start + 12.hours) do
      get "/"
      expect(response).to redirect_to("/login")
      expect(session[:user_id]).to be_nil
      follow_redirect!
      expect(response.body).to include("Your session expired. Sign in again.")
    end
  end

  it "isn't extended by activity" do
    start = Time.current.change(usec: 0)
    travel_to(start) { sign_in }

    [1, 6, 11].each do |hours|
      travel_to(start + hours.hours) do
        get "/"
        expect(response).to have_http_status(:ok)
      end
    end

    travel_to(start + 12.hours) do
      get "/"
      expect(response).to redirect_to("/login")
    end
  end

  it "expires a copied cookie too, and a new login gets a fresh lifetime" do
    start = Time.current.change(usec: 0)
    travel_to(start) { sign_in }
    copied = cookies[cookie_name]

    travel_to(start + 13.hours) do
      expect_sent_to_login(replay(copied))
      sign_in
      get "/"
      expect(response).to have_http_status(:ok)
      expect(session[:expires_at]).to eq((start + 25.hours).to_i)
    end
  end

  it "follows the configured lifetime" do
    Rails.configuration.x.session_lifetime_seconds = 1.hour.to_i
    start = Time.current.change(usec: 0)
    travel_to(start) { sign_in }

    travel_to(start + 59.minutes) do
      get "/"
      expect(response).to have_http_status(:ok)
    end
    travel_to(start + 1.hour) do
      get "/"
      expect(response).to redirect_to("/login")
    end
  end

  it "makes an OIDC user sign in again, which re-checks their groups" do
    use_oidc_mode(viewer_group: "rotten-viewers")
    mock_oidc_auth(uid: "sub-expiring", email: "oidc-expiring@example.test", groups: ["rotten-viewers"])
    start = Time.current.change(usec: 0)
    travel_to(start) do
      oidc_sign_in
      expect(response).to redirect_to("/")
    end

    travel_to(start + 12.hours) do
      get "/"
      expect(response).to redirect_to("/login")

      mock_oidc_auth(uid: "sub-expiring", email: "oidc-expiring@example.test", groups: ["someone-else"])
      oidc_sign_in
      expect(response).to have_http_status(:forbidden)
      get "/"
      expect(response).to redirect_to("/login")
    end
  end

  describe "a session from before expiry and generations existed" do
    let(:expires_at) { (Time.current + 1.hour).to_i }
    let(:generation) { user.session_generation }

    it "is accepted when it has both, so the cookies below are built right" do
      cookie = session_cookie("user_id" => user.id, "session_generation" => generation, "expires_at" => expires_at,
                              "credential" => user.credential_fingerprint)

      expect(replay(cookie)).to have_http_status(:ok)
    end

    {
      "neither" => {},
      "no expiry" => { "session_generation" => :generation },
      "no generation" => { "expires_at" => :expires_at },
      "a string expiry" => { "session_generation" => :generation, "expires_at" => :expires_at_string },
      "a string generation" => { "session_generation" => :generation_string, "expires_at" => :expires_at }
    }.each do |what, extra|
      it "is refused with #{what}" do
        values = { generation: generation, expires_at: expires_at,
                   generation_string: generation.to_s, expires_at_string: expires_at.to_s }
        data = { "user_id" => user.id, "credential" => user.credential_fingerprint }
        extra.each { |key, value| data[key] = values.fetch(value) }

        expect_sent_to_login(replay(session_cookie(data)))
      end
    end
  end
end
