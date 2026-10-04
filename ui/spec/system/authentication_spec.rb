require "rails_helper"

RSpec.describe "Authentication", type: :system do
  before do
    User.delete_all if defined?(User)
  end

  it "redirects a logged-out visitor to the login page" do
    visit "/"

    expect(page).to have_current_path("/login")
    expect(page).to have_css("h1", text: "Sign in to Rotten")
    expect(page).to have_text("password")
  end

  it "lets a signed-in user log out" do
    user = User.create!(email: "viewer-system@example.com", name: "Viewer User", role: "viewer", active: true)

    visit "/__test/sign_in?user_id=#{user.id}"
    visit "/"
    click_button "Log out"

    expect(page).to have_current_path("/login")
    expect(page).to have_text("Signed out")
  end

  it "sends a user whose session expired back to the login page, saying why" do
    user = User.create!(email: "expiring-system@example.com", name: "Expiring User", role: "viewer", active: true)

    visit "/__test/sign_in?user_id=#{user.id}"
    visit "/"
    expect(page).to have_button("Log out")

    travel 12.hours
    visit "/"

    expect(page).to have_current_path("/login")
    expect(page).to have_text("Your session expired. Sign in again.")
  end
end
