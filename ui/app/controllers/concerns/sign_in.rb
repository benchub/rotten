module SignIn
  extend ActiveSupport::Concern

  private

  # Every sign-in gets a fresh session, which prevents session fixation.
  def start_session(user)
    end_session
    session[:user_id] = user.id
    session[:credential] = user.credential_fingerprint
  end

  # True when the session was started with the user's current password, or
  # the user has no password at all (OIDC).
  def session_credential_current?(user)
    return true unless user.password_login?

    expected = user.credential_fingerprint
    expected.present? && ActiveSupport::SecurityUtils.secure_compare(session[:credential].to_s, expected)
  end

  # A refused sign-in also ends whatever session was there before.
  def end_session
    reset_session
    @current_user = nil
  end
end
