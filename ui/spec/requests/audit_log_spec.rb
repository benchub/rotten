require "rails_helper"

RSpec.describe "Audit log", :api_keys, type: :request do
  before { User.delete_all }

  let!(:admin) { User.create!(email: "audit.admin@example.test", role: "admin", active: true) }

  def sign_in(user)
    post "/__test/sign_in", params: { user_id: user.id }
  end

  def rows_in(body)
    Nokogiri::HTML(body).css("table.audit-log tbody tr")
  end

  def ids_in(body)
    rows_in(body).map { |tr| Integer(tr["data-audit-id"]) }
  end

  it "sends a signed-out user to the login page" do
    get "/admin/audit"

    expect(response).to redirect_to("/login")
  end

  it "shows admins every field, newest first" do
    older = owner_insert_audit_row(action: "api_key.create", actor_email: "first@example.test", actor_user_id: 7,
                                   target_type: "api_key", target_id: 3, details: { name: "worker-db1" },
                                   at: Time.utc(2026, 9, 1, 12, 0))
    newer = owner_insert_audit_row(action: "user.disable", actor_email: "rake", target_type: "user", target_id: 9,
                                   details: { email: "gone@example.test" }, at: Time.utc(2026, 9, 2, 13, 30))
    sign_in(admin)

    get "/admin/audit"

    expect(response).to have_http_status(:ok)
    expect(ids_in(response.body)).to eq([newer, older])
    first, second = rows_in(response.body).map { |tr| tr.css("td").map { |td| td.text.strip } }
    expect(first).to eq(["2026-09-02 13:30:00 UTC", "rake", "user.disable", "user 9", '{"email":"gone@example.test"}'])
    expect(second).to eq(["2026-09-01 12:00:00 UTC", "first@example.test (user 7)", "api_key.create", "api_key 3",
                          '{"name":"worker-db1"}'])
  end

  it "says so when the log is empty" do
    sign_in(admin)

    get "/admin/audit"

    expect(response).to have_http_status(:ok)
    expect(rows_in(response.body)).to be_empty
    expect(response.body).to include("No audit entries")
  end

  describe "paging" do
    before { stub_const("AuditLogsController::PAGE_SIZE", 2) }

    let!(:ids) { Array.new(5) { |i| owner_insert_audit_row(action: "api_key.create", details: { n: i }) } }

    it "pages back through older entries with before, and links only while there are more" do
      sign_in(admin)

      get "/admin/audit"
      expect(ids_in(response.body)).to eq(ids.last(2).reverse)
      older = Nokogiri::HTML(response.body).at_css("a[rel=next]")
      expect(older["href"]).to eq("/admin/audit?before=#{ids[3]}")

      get older["href"]
      expect(ids_in(response.body)).to eq([ids[2], ids[1]])
      older = Nokogiri::HTML(response.body).at_css("a[rel=next]")
      expect(older["href"]).to eq("/admin/audit?before=#{ids[1]}")

      get older["href"]
      expect(ids_in(response.body)).to eq([ids[0]])
      expect(Nokogiri::HTML(response.body).at_css("a[rel=next]")).to be_nil
      expect(Nokogiri::HTML(response.body).at_css("a[href='/admin/audit']")).to be_present
    end

    it "shows no older link when the last page is exactly full" do
      sign_in(admin)

      get "/admin/audit", params: { before: ids[2] }

      expect(ids_in(response.body)).to eq([ids[1], ids[0]])
      expect(Nokogiri::HTML(response.body).at_css("a[rel=next]")).to be_nil
    end

    ["", "0", "-1", "abc", "1.5", "1e3", " 3", "3 ", "9" * 19, "01"].each do |before|
      it "rejects before=#{before.inspect} with 400" do
        sign_in(admin)

        get "/admin/audit", params: { before: before }

        expect(response).to have_http_status(:bad_request)
      end
    end

    it "rejects a before given twice, as an array" do
      sign_in(admin)

      get "/admin/audit?before[]=3"

      expect(response).to have_http_status(:bad_request)
    end
  end
end
