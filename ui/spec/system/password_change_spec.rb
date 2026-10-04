require "rails_helper"

RSpec.describe "Changing your own password", type: :system do
  let(:password) { "system-change-password-1" }
  let(:new_password) { "system-change-password-2" }

  before do
    User.delete_all
  end

  def sign_in_through_form(email, with_password)
    visit "/login"
    fill_in "Email", with: email
    fill_in "Password", with: with_password
    click_button "Sign in"
    expect(page).to have_text("Signed in as #{email}")
  end

  def fill_in_change(current:, new:, confirmation: new)
    fill_in "Current password", with: current
    fill_in "New password", with: new
    fill_in "Confirm new password", with: confirmation
    click_button "Change password"
  end

  it "lets a password user change their password from the home page and stay signed in" do
    user = create_password_user(email: "system.changer@example.test", password: password)
    sign_in_through_form(user.email, password)

    click_link "Change password"
    expect(page).to have_css("h1", text: "Change password")
    fill_in_change(current: password, new: new_password)

    expect(page).to have_current_path("/")
    expect(page).to have_text("Password changed")
    expect(page).to have_text("Signed in as #{user.email}")
    visit "/"
    expect(page).to have_text("Signed in as #{user.email}")

    click_button "Log out"
    sign_in_through_form(user.email, new_password)
  end

  it "shows a generic message for a wrong current password and clears every field" do
    user = create_password_user(email: "system.changer@example.test", password: password)
    sign_in_through_form(user.email, password)

    visit "/password"
    fill_in_change(current: "not-the-password", new: new_password)

    expect(page).to have_text(PasswordsController::WRONG_CURRENT_PASSWORD)
    expect(page).to have_field("Current password", with: "")
    expect(page).to have_field("New password", with: "")
    expect(page).to have_field("Confirm new password", with: "")
    visit "/"
    expect(page).to have_text("Signed in as #{user.email}")
    expect(user.reload.authenticate(password)).to eq(user)
  end

  it "explains a new password that's too short" do
    user = create_password_user(email: "system.changer@example.test", password: password)
    sign_in_through_form(user.email, password)

    visit "/password"
    fill_in_change(current: password, new: "short")

    expect(page).to have_text("too short")
    expect(user.reload.authenticate(password)).to eq(user)
  end

  it "doesn't offer the page to an OIDC user" do
    user = User.create!(email: "system.sso@example.test", role: "viewer", provider: oidc_provider,
                        provider_uid: "sub-system")
    visit "/__test/sign_in?user_id=#{user.id}"

    visit "/"
    expect(page).to have_text("Signed in as #{user.email}")
    expect(page).to have_no_link("Change password")
  end
end
