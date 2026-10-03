require "rails_helper"

RSpec.describe "Authentication", type: :request do
  before do
    User.delete_all if defined?(User)
  end

  it "redirects logged-out requests to /login" do
    get "/"

    expect(response).to redirect_to("/login")
  end

  it "drops the session for an inactive user on the next request" do
    user = User.create!(email: "inactive@example.com", name: "Inactive User", role: "viewer", active: false)

    post "/__test/sign_in", params: { user_id: user.id }
    get "/"

    expect(response).to redirect_to("/login")
    expect(session[:user_id]).to be_nil
  end

  it "drops the session for an inactive user even on a login page request" do
    user = User.create!(email: "inactive-login@example.com", name: "Inactive User", role: "viewer", active: false)

    post "/__test/sign_in", params: { user_id: user.id }
    get "/login"

    expect(response).to have_http_status(:ok)
    expect(session[:user_id]).to be_nil
  end

  it "returns forbidden when a viewer visits an admin page" do
    user = User.create!(email: "viewer@example.com", name: "Viewer User", role: "viewer", active: true)

    post "/__test/sign_in", params: { user_id: user.id }
    get "/admin"

    expect(response).to have_http_status(:forbidden)
  end

  it "does not expose the test sign-in route outside the test environment" do
    allow(Rails.env).to receive(:test?).and_return(false)

    post "/__test/sign_in", params: { user_id: 1 }

    expect(response).to have_http_status(:not_found)
  end
end
