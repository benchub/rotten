class ApplicationController < ActionController::Base
  include SignIn

  before_action :drop_revoked_session
  before_action :require_login

  # Only allow modern browsers supporting webp images, web push, badges, import maps, CSS nesting, and CSS :has.
  allow_browser versions: :modern

  # Changes to the importmap will invalidate the etag for HTML responses
  stale_when_importmap_changes

  helper_method :current_user

  private

  def current_user
    return nil unless session[:user_id]

    @current_user ||= User.find_by(id: session[:user_id])
  end

  def require_login
    if current_user
      return
    end

    redirect_to login_path
  end

  def require_admin
    head :forbidden unless current_user.admin?
  end

  # A disabled user, or a password user whose password changed since the
  # session started, loses the session on the next request.
  def drop_revoked_session
    return unless current_user
    return if current_user.active? && session_credential_current?(current_user)

    end_session
  end
end
