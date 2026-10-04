require "rails_helper"

RSpec.describe "Audit log", :api_keys, type: :system do
  before { User.delete_all }

  def sign_in(role)
    user = User.create!(email: "audit.#{role}@example.test", role: role, active: true)
    visit "/__test/sign_in?user_id=#{user.id}"
    user
  end

  it "lets an admin read the log from the admin page and page back" do
    stub_const("AuditLogsController::PAGE_SIZE", 1)
    owner_insert_audit_row(action: "api_key.create", actor_email: "keys@example.test", target_type: "api_key",
                           target_id: 4, details: { name: "worker-db1" })
    owner_insert_audit_row(action: "user.disable", actor_email: "rake", target_type: "user", target_id: 9)
    sign_in("admin")

    visit "/admin"
    click_link "Audit log"

    expect(page).to have_css("h1", text: "Audit log")
    expect(page).to have_text("user.disable")
    expect(page).to have_no_text("api_key.create")

    click_link "Older"
    expect(page).to have_text("api_key.create")
    expect(page).to have_text("worker-db1")
    expect(page).to have_no_text("user.disable")
    expect(page).to have_no_link("Older")

    click_link "Newest"
    expect(page).to have_text("user.disable")
  end

  it "doesn't show viewers the page" do
    owner_insert_audit_row(action: "user.disable", actor_email: "rake")
    sign_in("viewer")

    visit "/admin/audit"

    expect(page).to have_no_css("h1", text: "Audit log")
    expect(page).to have_no_text("user.disable")
  end
end
