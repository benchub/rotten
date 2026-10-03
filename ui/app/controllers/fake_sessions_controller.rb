class FakeSessionsController < ApplicationController
  include OidcSignIn

  skip_before_action :require_login
  before_action :require_fake_login

  def create
    sign_in_with_oidc(RottenUi::FakeLogin.auth_hash(params.require(:persona), Rails.configuration.x.oidc), provider: "fake")
  end

  private

  # The route is only drawn in development; this is a second gate.
  def require_fake_login
    head :not_found unless Rails.env.development? && Rails.configuration.x.fake_login &&
      RottenUi::FakeLogin::PERSONAS.key?(params[:persona])
  end
end
