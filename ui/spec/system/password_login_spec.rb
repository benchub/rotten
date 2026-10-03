require "rails_helper"

RSpec.describe "Password login", type: :system do
  let(:password) { "system-spec-password-123" }

  before do
    User.delete_all
  end

  it "signs a user in with email and password, and back out" do
    create_password_user(email: "system.viewer@example.test", password: password)

    visit "/"
    expect(page).to have_current_path("/login")
    fill_in "Email", with: "System.Viewer@example.test"
    fill_in "Password", with: password
    click_button "Sign in"

    expect(page).to have_current_path("/")
    expect(page).to have_text("Signed in as system.viewer@example.test")

    click_button "Log out"
    expect(page).to have_current_path("/login")
    expect(page).to have_text("Signed out")
    visit "/"
    expect(page).to have_current_path("/login")
  end

  it "shows a generic message for a wrong password and keeps the email" do
    create_password_user(email: "system.viewer@example.test", password: password)

    visit "/login"
    fill_in "Email", with: "system.viewer@example.test"
    fill_in "Password", with: "not-the-password"
    click_button "Sign in"

    expect(page).to have_text(SessionsController::FAILURE)
    expect(page).to have_field("Email", with: "system.viewer@example.test")
    expect(page).to have_field("Password", with: "")
    visit "/"
    expect(page).to have_current_path("/login")
  end
end
