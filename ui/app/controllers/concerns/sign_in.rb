module SignIn
  extend ActiveSupport::Concern

  private

  # Every sign-in gets a fresh session, which prevents session fixation. The
  # session records the user's session_generation, which a bump moves past,
  # and an absolute expiry that activity never extends.
  def start_session(user)
    end_session
    session[:user_id] = user.id
    session[:credential] = user.credential_fingerprint
    session[:session_generation] = user.session_generation
    session[:expires_at] = Time.current.to_i + Rails.configuration.x.session_lifetime_seconds
  end

  # True when the user may no longer use this session: they're disabled,
  # their password changed, or their sessions were revoked since it started.
  # A session without a generation, from before generations existed, counts
  # as revoked. It's all read from the user row already loaded, so the check
  # costs no query.
  def session_revoked?(user)
    generation = session[:session_generation]
    !user.active? || !session_credential_current?(user) ||
      !(generation.is_a?(Integer) && generation == user.session_generation)
  end

  # True once the session's lifetime has run out, or when it has no expiry
  # at all because it started before expiries existed.
  def session_expired?
    expires_at = session[:expires_at]
    !expires_at.is_a?(Integer) || Time.current.to_i >= expires_at
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
