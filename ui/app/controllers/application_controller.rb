class ApplicationController < ActionController::Base
  include SignIn

  before_action :drop_revoked_session
  before_action :require_login

  # Only allow modern browsers supporting webp images, web push, badges, import maps, CSS nesting, and CSS :has.
  allow_browser versions: :modern

  # Changes to the importmap will invalidate the etag for HTML responses
  stale_when_importmap_changes

  helper_method :current_user

  SESSION_EXPIRED = "Your session expired. Sign in again.".freeze

  private

  def current_user
    return nil unless session[:user_id]

    @current_user ||= User.find_by(id: session[:user_id])
  end

  def require_login
    if current_user
      return
    end

    if @session_expired
      redirect_to login_path, alert: SESSION_EXPIRED
    else
      redirect_to login_path
    end
  end

  def require_admin
    head :forbidden unless current_user.admin?
  end

  # A revoked session (see SignIn#session_revoked?) or an expired one is
  # dropped on its next request. Pages that need a login then send the user
  # to /login, saying why when the session simply expired.
  def drop_revoked_session
    return unless current_user

    if session_revoked?(current_user)
      end_session
    elsif session_expired?
      end_session
      @session_expired = true
    end
  end
end
