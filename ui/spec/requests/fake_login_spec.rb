require "rails_helper"

# The fake login route is only drawn when booting in development, which the
# boot spec covers. Here it is drawn by hand so the controller's own gate and
# the persona flow can be exercised in-process.
RSpec.describe "Fake login", type: :request do
  around do |example|
    fake_login = Rails.configuration.x.fake_login
    Rails.application.routes.disable_clear_and_finalize = true
    Rails.application.routes.draw do
      post "auth/fake/:persona" => "fake_sessions#create", as: :fake_login
    end
    example.run
  ensure
    Rails.application.routes.disable_clear_and_finalize = false
    Rails.configuration.x.fake_login = fake_login
    Rails.application.reload_routes!
  end

  before do
    User.delete_all
    use_oidc_mode(viewer_group: "dev-viewers", admin_group: "dev-admins")
    Rails.configuration.x.fake_login = true
  end

  context "in development" do
    before { allow(Rails.env).to receive(:development?).and_return(true) }

    it "signs in the admin persona as an admin" do
      post "/auth/fake/admin"

      expect(response).to redirect_to("/")
      expect(User.sole).to have_attributes(provider: "fake", provider_uid: "fake-admin", role: "admin",
                                           groups: %w[dev-viewers dev-admins])
      expect(session[:user_id]).to eq(User.sole.id)
    end

    it "signs in the viewer persona as a viewer" do
      post "/auth/fake/viewer"

      expect(User.sole.role).to eq("viewer")
      get "/admin"
      expect(response).to have_http_status(:forbidden)
    end

    it "makes the admin persona a viewer when no admin group is configured" do
      use_oidc_mode(viewer_group: "dev-viewers")

      post "/auth/fake/admin"

      expect(User.sole.role).to eq("viewer")
    end

    it "rejects unknown personas" do
      post "/auth/fake/root"

      expect(response).to have_http_status(:not_found)
      expect(User.count).to eq(0)
    end

    it "lists both personas on the login page" do
      get "/login"

      expect(response.body).to include("Sign in as fake viewer", "Sign in as fake admin")
    end
  end

  it "refuses even a drawn route outside development" do
    post "/auth/fake/admin"

    expect(response).to have_http_status(:not_found)
    expect(User.count).to eq(0)
    expect(session[:user_id]).to be_nil
  end
end
