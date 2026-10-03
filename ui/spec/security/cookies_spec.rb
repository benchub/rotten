require "rails_helper"

# Flags on the session cookie in the test env. The Secure flag comes from
# force_ssl, so spec/security/production_boot_spec.rb checks it in production.
RSpec.describe "Session cookie flags", type: :request do
  let(:cookie_name) { Rails.application.config.session_options.fetch(:key) }
  let(:password) { "cookie-spec-password-123" }

  before { User.delete_all }

  def session_set_cookie
    lines = Array(response.headers["set-cookie"]).flat_map { |header| header.split("\n") }
    line = lines.find { |cookie| cookie.start_with?("#{cookie_name}=") }
    expect(line).to be_present, "no #{cookie_name} cookie in #{lines.inspect}"
    line.split(";").map(&:strip).drop(1).map(&:downcase)
  end

  it "is HttpOnly and SameSite=Lax when a user signs in" do
    user = create_password_user(email: "cookie@example.test", password: password)

    password_sign_in(email: user.email, password: password)

    expect(response).to redirect_to("/")
    expect(session_set_cookie).to include("httponly", "samesite=lax", "path=/")
  end

  it "is encrypted, so the browser can't read or forge the user ID" do
    user = create_password_user(email: "cookie@example.test", password: password)

    password_sign_in(email: user.email, password: password)

    raw = CGI.unescape(cookies[cookie_name])
    expect(raw).not_to include("user_id")
    expect(Base64.decode64(raw.split("--").first)).not_to include("user_id")

    tampered = open_session
    tampered.cookies[cookie_name] = Base64.strict_encode64({ "user_id" => user.id }.to_json)
    tampered.get "/"
    expect(tampered.response).to have_http_status(:found)
    expect(URI(tampered.response.location).path).to eq("/login")
  end
end
