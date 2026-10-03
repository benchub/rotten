Rails.application.routes.draw do
  root "home#show"

  get "up" => "health#show", as: :rails_health_check
  get "login" => "sessions#new"
  # 404s unless ROTTEN_UI_AUTH=password.
  post "login" => "sessions#create"
  delete "logout" => "sessions#destroy"
  get "admin" => "admin#show"
  get "reports" => "reports#index", as: :reports, format: false
  get "reports/:id" => "reports#show", as: :report, format: false
  # Any id reaches the controller, which 404s the ones that aren't a
  # fingerprint.
  get "fingerprints/:id" => "fingerprints#show", as: :fingerprint, format: false, constraints: { id: %r{[^/]+} }

  # OmniAuth's middleware handles POST /auth/openid_connect and hands the
  # callback to this route.
  get "auth/openid_connect/callback" => "oidc_callbacks#create", as: :oidc_callback
  get "auth/failure" => "oidc_callbacks#failure", as: :auth_failure

  if Rails.configuration.x.fake_login
    post "auth/fake/:persona" => "fake_sessions#create", as: :fake_login, constraints: { persona: /viewer|admin/ }
  end

  constraints ->(_) { Rails.env.test? } do
    get "__test/sign_in" => "test_sessions#create"
    post "__test/sign_in" => "test_sessions#create"
  end
end
