require "rails_helper"

RSpec.describe "OIDC login", type: :system do
  before do
    User.delete_all
    use_oidc_mode(viewer_group: "rotten-viewers", admin_group: "rotten-admins")
  end

  it "signs a user in through the identity provider and back out" do
    mock_oidc_auth(uid: "sub-system", email: "system.viewer@example.test", name: "System Viewer", groups: ["rotten-viewers"])

    visit "/"
    expect(page).to have_current_path("/login")
    click_button "Sign in"

    expect(page).to have_current_path("/")
    expect(page).to have_text("Signed in as system.viewer@example.test")

    click_button "Log out"
    expect(page).to have_current_path("/login")
    expect(page).to have_text("Signed out")
  end

  it "shows a 403 page to someone in neither group" do
    mock_oidc_auth(uid: "sub-system-outsider", email: "outsider@example.test", groups: ["elsewhere"])

    visit "/login"
    click_button "Sign in"

    expect(page).to have_link("Back to sign in")
    expect(page).to have_text("not in a group that can use Rotten")
    visit "/"
    expect(page).to have_current_path("/login")
  end

  it "shows a friendly message when the identity provider reports an error" do
    OmniAuth.config.mock_auth[:openid_connect] = :access_denied

    visit "/login"
    click_button "Sign in"

    expect(page).to have_current_path("/login")
    expect(page).to have_text("Sign-in didn't work")
    expect(page).not_to have_text("access_denied")
  end
end
