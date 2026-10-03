Rails.application.routes.draw do
  root "home#show"

  get "up" => "health#show", as: :rails_health_check
  get "login" => "sessions#new"
  delete "logout" => "sessions#destroy"
  get "admin" => "admin#show"

  constraints ->(_) { Rails.env.test? } do
    get "__test/sign_in" => "test_sessions#create"
    post "__test/sign_in" => "test_sessions#create"
  end
end
