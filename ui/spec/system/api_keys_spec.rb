require "rails_helper"

RSpec.describe "Pass key admin", :api_keys, type: :system do
  before do
    User.delete_all
    browser_logs
  end

  def sign_in(role)
    user = User.create!(email: "keys.#{role}@example.test", name: role, role: role, active: true)
    visit "/__test/sign_in?user_id=#{user.id}"
    user
  end

  def browser_logs
    page.driver.browser.logs.get(:browser).map(&:message)
  end

  it "lets an admin create a key, see its token once, find it in the list, and revoke it" do
    admin = sign_in("admin")

    visit "/"
    click_link "Pass keys"
    expect(page).to have_css("h1", text: "Pass keys")
    click_link "New pass key"

    fill_in "Name", with: "worker-db1"
    fill_in "FQDN", with: "db1.example.test"
    click_button "Create pass key"

    expect(page).to have_text("Copy this pass key now")
    token = find("#pass-key-token").text
    expect(token).to match(/\Arotten_\d+_[A-Za-z0-9_-]{43}\z/)
    id = token[/\Arotten_(\d+)_/, 1]

    click_link "Back to pass keys"
    expect(page).to have_css("h1", text: "Pass keys")
    expect(page).not_to have_text(token)
    within("tr[data-key-id='#{id}']") do
      expect(page).to have_text("worker-db1")
      expect(page).to have_text("db1.example.test")
      expect(page).to have_text(admin.email)
      expect(page).to have_text("Active")
      accept_confirm("Revoke worker-db1? Workers using it will be refused.") do
        click_button "Revoke"
      end
    end

    expect(page).to have_text("Revoked worker-db1")
    within("tr[data-key-id='#{id}']") do
      expect(page).to have_text("Revoked")
      expect(page).to have_text(admin.email)
      expect(page).to have_no_button("Revoke")
    end

    expect(owner_audit_rows.map { |r| r["action"] }).to eq(%w[api_key.create api_key.revoke])
    expect(browser_logs.grep(/Content Security Policy/i)).to be_empty
  end

  # no-store keeps the created page out of the browser's cache, but not out of
  # Turbo's snapshot cache, which would bring the token back on Back.
  def create_key_for_back_test(name)
    sign_in("admin")
    visit "/admin/keys/new"
    fill_in "Name", with: name
    fill_in "FQDN", with: "back.example.test"
    click_button "Create pass key"
    token = find("#pass-key-token").text
    expect(token).to start_with("rotten_")
    token
  end

  # Turbo restores on popstate after go_back returns, so wait for the page to
  # be replaced before looking for the token.
  def go_back_and_wait
    page.execute_script("document.body.dataset.beforeBack = 'true'")
    page.go_back
    expect(page).to have_no_css("body[data-before-back]")
  end

  it "doesn't show the token again after Back to pass keys and browser Back" do
    token = create_key_for_back_test("worker-back")

    click_link "Back to pass keys"
    expect(page).to have_css("h1", text: "Pass keys")
    go_back_and_wait

    expect(page).to have_no_css("#pass-key-token")
    expect(page).to have_no_text(token)
    expect(owner_api_key_count).to eq(1)
  end

  # Any Turbo navigation away from the created page, standing in for links the
  # page may gain, caches its snapshot unless the page opts out.
  it "doesn't let Turbo restore the token from its snapshot cache" do
    token = create_key_for_back_test("worker-snapshot")

    page.execute_script("Turbo.visit('/')")
    expect(page).to have_css("h1", text: "Rotten")
    go_back_and_wait

    expect(page).to have_no_css("#pass-key-token")
    expect(page).to have_no_text(token)
    expect(owner_api_key_count).to eq(1)
  end

  it "shows what's wrong with the form and keeps what was typed" do
    sign_in("admin")

    visit "/admin/keys/new"
    fill_in "Name", with: "worker db1"
    fill_in "FQDN", with: "db1..example.test"
    click_button "Create pass key"

    expect(page).to have_text("Name may only contain")
    expect(page).to have_text("FQDN must be a host name")
    expect(page).to have_field("Name", with: "worker db1")
    expect(page).not_to have_css("#pass-key-token")
    expect(owner_api_key_count).to eq(0)
  end

  it "doesn't show viewers the link or the pages" do
    sign_in("viewer")

    visit "/"
    expect(page).to have_no_link("Pass keys")

    visit "/admin/keys"
    expect(page).to have_no_css("h1", text: "Pass keys")
  end
end
