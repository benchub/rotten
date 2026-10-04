class SessionsController < ApplicationController
  # One message for every failed password login: a wrong password, an unknown
  # email, a disabled user and an OIDC user all look the same.
  FAILURE = "That email and password didn't work. Try again, or ask your administrator for help.".freeze
  TOO_MANY_ATTEMPTS = "Too many sign-in attempts. Wait a few minutes and try again.".freeze

  # Counted in Rails.cache, a memory store, so each app process keeps its own
  # counts: N Puma workers or replicas allow N times these limits. See "Login
  # rate limits" in docs/ui.md, whose smoke check reads these constants.
  # Every POST counts, successful or not.
  ATTEMPTS_PER_IP = 10
  ATTEMPTS_PER_EMAIL = 5
  ATTEMPTS_WINDOW = 3.minutes

  skip_before_action :require_login, only: %i[new create]
  before_action :require_password_mode, only: :create
  rate_limit to: ATTEMPTS_PER_IP, within: ATTEMPTS_WINDOW, only: :create, name: "ip",
             with: :too_many_attempts
  rate_limit to: ATTEMPTS_PER_EMAIL, within: ATTEMPTS_WINDOW, only: :create, name: "email",
             by: :email_rate_limit_key, with: :too_many_attempts

  def new
  end

  def create
    user = User.authenticate_password_login(email: params[:email], password: params[:password])

    if user
      user.update!(last_login_at: Time.current)
      start_session(user)
      redirect_to root_path, notice: "Signed in"
    else
      end_session
      render_login(FAILURE, :unprocessable_content)
    end
  end

  # Ends every session the user has, on any device, including copies of this
  # one's cookie, not just this browser's.
  def destroy
    current_user.revoke_sessions!
    end_session
    redirect_to login_path, notice: "Signed out"
  end

  private

  def require_password_mode
    head :not_found unless Rails.configuration.x.auth_mode == "password"
  end

  def submitted_email
    email = params[:email]
    email.is_a?(String) ? User.normalize_value_for(:email, email) : ""
  end

  # Hashed so the cache key has a fixed length whatever was submitted.
  def email_rate_limit_key
    Digest::SHA256.hexdigest(submitted_email)
  end

  def too_many_attempts
    render_login(TOO_MANY_ATTEMPTS, :too_many_requests)
  end

  def render_login(message, status)
    @email = submitted_email
    flash.now[:alert] = message
    render :new, status: status
  end
end
