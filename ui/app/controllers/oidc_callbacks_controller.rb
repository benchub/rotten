class OidcCallbacksController < ApplicationController
  include OidcSignIn

  FAILURE = "Sign-in didn't work. Try again, or ask your administrator for help.".freeze

  skip_before_action :require_login
  before_action :require_oidc_mode

  def create
    auth = request.env["omniauth.auth"]
    return redirect_to(auth_failure_path) unless auth

    sign_in_with_oidc(auth, provider: Rails.configuration.x.oidc.provider)
  end

  def failure
    redirect_to login_path, alert: FAILURE
  end

  private

  def require_oidc_mode
    head :not_found unless Rails.configuration.x.auth_mode == "oidc"
  end
end
