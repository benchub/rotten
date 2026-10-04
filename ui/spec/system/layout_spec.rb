require "rails_helper"

# The shared top bar: the app name, the main nav (Admin only for admins), the
# signed-in user with a role badge, and Log out, on every page behind a login.
RSpec.describe "Layout", :api_keys, type: :system do
  let(:password) { "layout-spec-password-1" }

  before do
    User.delete_all
    @fixture = ReportFixture.seed!
  end

  def sign_in(role)
    user = create_password_user(email: "layout.#{role}@example.test", password: password, role: role)
    visit "/__test/sign_in?user_id=#{user.id}"
    expect(page).to have_text("Signed in as #{user.email}")
    user
  end

  def run_params
    "project=canvas&environment=production&cluster=13&range=3h"
  end

  def viewer_pages
    fingerprint = @fixture.fingerprint_ids.fetch("users")
    {
      "/" => [["h1", "Welcome to Rotten"], nil],
      "/reports" => [["h1", "Reports"], "Reports"],
      "/reports?report=top_by_calls&#{run_params}" => [["h2.report-title", "Top queries by calls"], "Reports"],
      "/fingerprints/#{fingerprint}?#{run_params}" => [["h1", "Fingerprint #{fingerprint}"], "Reports"],
      "/password" => [["h1", "Change password"], nil]
    }
  end

  def admin_pages
    viewer_pages.merge(
      "/admin" => [["h1", "Admin"], "Admin"],
      "/admin/keys" => [["h1", "Pass keys"], "Admin"],
      "/admin/keys/new" => [["h1", "New pass key"], "Admin"],
      "/admin/audit" => [["h1", "Audit log"], "Admin"]
    )
  end

  # heading is [selector, text] only that page has, so the check waits for the page.
  def expect_top_bar(user, heading:, current:, admin:)
    expect(page).to have_css(heading.first, text: heading.last)
    expect(page).to have_title(/rotten\z/)
    within("header.topbar") do
      expect(page).to have_link("rotten", href: "/")
      within("nav[aria-label='Main']") do
        expect(page).to have_link("Reports", href: "/reports")
        if admin
          expect(page).to have_link("Admin", href: "/admin")
        else
          expect(page).to have_no_link("Admin")
        end
        if current
          expect(page).to have_css("a[aria-current='page']", count: 1, text: current)
        else
          expect(page).to have_no_css("a[aria-current]")
        end
      end
      expect(page).to have_text(user.email)
      expect(page).to have_css(".badge", text: user.role)
      expect(page).to have_button("Log out")
    end
  end

  it "shows a viewer the top bar on every page, without Admin" do
    viewer = sign_in("viewer")

    viewer_pages.each do |path, (heading, current)|
      visit path
      expect_top_bar(viewer, heading: heading, current: current, admin: false)
    end
  end

  it "shows an admin the top bar on every page, with Admin, including the one-time pass key page" do
    admin = sign_in("admin")

    admin_pages.each do |path, (heading, current)|
      visit path
      expect_top_bar(admin, heading: heading, current: current, admin: true)
    end

    visit "/admin/keys/new"
    fill_in "Name", with: "layout-db1"
    fill_in "FQDN", with: "db1.example.test"
    click_button "Create pass key"
    expect_top_bar(admin, heading: ["h1", "Pass key created"], current: "Admin", admin: true)
  end

  it "logs out from the top bar" do
    sign_in("viewer")
    visit "/reports"
    within("header.topbar") { click_button "Log out" }

    expect(page).to have_current_path("/login")
    expect(page).to have_text("Signed out")
  end

  it "shows the app name but no nav or user before sign-in" do
    visit "/login"

    expect(page).to have_css("h1", text: "Sign in to Rotten")
    expect(page).to have_title("Sign in · rotten")
    within("header.topbar") do
      expect(page).to have_link("rotten")
      expect(page).to have_no_css("nav[aria-label='Main']")
      expect(page).to have_no_button("Log out")
    end
  end
end
