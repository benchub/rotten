# Lets a signed-in password user change their own password. OIDC users, and
# everyone in oidc mode, get a 404.
class PasswordsController < ApplicationController
  # One message for every current-password failure: wrong, missing, or not
  # a string.
  WRONG_CURRENT_PASSWORD = "Your current password wasn't right. Try again.".freeze
  TOO_MANY_ATTEMPTS = "Too many password change attempts. Wait a few minutes and try again.".freeze

  # The same limits and mechanism as login, but counted separately from it.
  # Every PATCH counts, successful or not.
  ATTEMPTS_PER_IP = SessionsController::ATTEMPTS_PER_IP
  ATTEMPTS_PER_USER = SessionsController::ATTEMPTS_PER_EMAIL
  ATTEMPTS_WINDOW = SessionsController::ATTEMPTS_WINDOW

  before_action :require_password_user
  rate_limit to: ATTEMPTS_PER_IP, within: ATTEMPTS_WINDOW, only: :update, name: "ip", with: :too_many_attempts
  rate_limit to: ATTEMPTS_PER_USER, within: ATTEMPTS_WINDOW, only: :update, name: "user",
             by: :user_rate_limit_key, with: :too_many_attempts

  def edit
  end

  # Checks the current password before looking at the new one. A wrong one
  # costs one bcrypt hash, as a failed login does. On success the user's
  # session_generation is bumped, which ends every session they have,
  # including a copy of this one's old cookie. This session is then reset, for
  # a new session ID, and restarted with the new generation and credential
  # fingerprint, so this browser stays signed in with a fresh lifetime: the
  # current password was just checked, as at login.
  def update
    user = User.authenticate_password_login(email: current_user.email, password: params[:current_password])
    return render_edit(WRONG_CURRENT_PASSWORD, :unprocessable_content) unless user&.id == current_user.id

    if user.change_password(params[:password], params[:password_confirmation])
      start_session(user)
      redirect_to root_path, notice: "Password changed."
    else
      @errors = user.errors.full_messages
      render_edit(nil, :unprocessable_content)
    end
  end

  private

  def require_password_user
    head :not_found unless Rails.configuration.x.auth_mode == "password" && current_user.password_login?
  end

  def user_rate_limit_key
    current_user.id.to_s
  end

  def too_many_attempts
    render_edit(TOO_MANY_ATTEMPTS, :too_many_requests)
  end

  def render_edit(message, status)
    flash.now[:alert] = message if message
    render :edit, status: status
  end
end
