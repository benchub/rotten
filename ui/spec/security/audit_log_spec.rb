require "rails_helper"

# The audit log is admin-only, and its rows carry strings users chose, such
# as key names and emails, so the page must render them as text.
RSpec.describe "Audit log security", :api_keys, type: :request do
  before { User.delete_all }

  def sign_in(role)
    user = User.create!(email: "audit.#{role}@example.test", role: role, active: true)
    post "/__test/sign_in", params: { user_id: user.id }
  end

  it "forbids viewers, whatever the params" do
    id = owner_insert_audit_row(action: "api_key.create", details: { name: "secret-ish" })
    sign_in("viewer")

    ["/admin/audit", "/admin/audit?before=#{id + 1}", "/admin/audit?before=junk"].each do |path|
      get path
      expect(response).to have_http_status(:forbidden), "#{path}: got #{response.status}"
      expect(response.body).not_to include("secret-ish")
    end
  end

  it "renders hostile values escaped" do
    hostile = "<script>window.pwned = 1</script>"
    owner_insert_audit_row(action: "api_key.create", actor_email: "<img src=x onerror=alert(1)>@example.test",
                           target_type: "</td><b>type</b>", details: { name: hostile, "<i>key</i>" => "x" })
    sign_in("admin")

    get "/admin/audit"

    expect(response).to have_http_status(:ok)
    body = response.body
    expect(body).not_to include("<script>window.pwned")
    expect(body).not_to include("<img src=x")
    expect(body).not_to include("<b>type</b>")
    expect(body).not_to include("<i>key</i>")
    expect(body).to include("&lt;script&gt;window.pwned = 1&lt;/script&gt;")
    expect(body).to include("&lt;img src=x onerror=alert(1)&gt;@example.test")
    expect(body).to include("&lt;/td&gt;&lt;b&gt;type&lt;/b&gt;")
  end
end
