class ApplicationController < ActionController::Base
  before_action :drop_inactive_session
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

  def drop_inactive_session
    return unless current_user && !current_user.active?

    reset_session
    @current_user = nil
  end
end
