require "rails_helper"

RSpec.describe "Pass key admin", :api_keys, type: :request do
  let(:password) { "api-keys-spec-password-123" }

  before { User.delete_all }

  let!(:admin) { create_password_user(email: "keys.admin@example.test", password: password, role: "admin") }
  let!(:viewer) { create_password_user(email: "keys.viewer@example.test", password: password) }

  def sign_in(user)
    post "/__test/sign_in", params: { user_id: user.id }
  end

  def create_key(name: "worker-db1", fqdn: "db1.example.test")
    post "/admin/keys", params: { api_key: { name: name, fqdn: fqdn } }
  end

  def token_in(body)
    Nokogiri::HTML(body).at_css("#pass-key-token")&.text&.strip
  end

  # Each key admin route, as [verb, path].
  def key_routes(id)
    [
      [:get, "/admin/keys"],
      [:get, "/admin/keys/new"],
      [:post, "/admin/keys"],
      [:post, "/admin/keys/#{id}/revoke"]
    ]
  end

  describe "access" do
    let!(:key_id) { owner_create_api_key(name: "existing") }

    it "sends a signed-out user to the login page from every route" do
      key_routes(key_id).each do |verb, path|
        process(verb, path, params: verb == :post ? { api_key: { name: "x", fqdn: "h" } } : {})
        expect(response).to redirect_to("/login"), "#{verb.upcase} #{path}: got #{response.status}"
      end
    end

    it "forbids viewers on every route and changes nothing" do
      sign_in(viewer)

      key_routes(key_id).each do |verb, path|
        process(verb, path, params: verb == :post ? { api_key: { name: "viewer-key", fqdn: "h.example.test" } } : {})
        expect(response).to have_http_status(:forbidden), "#{verb.upcase} #{path}: got #{response.status}"
      end
      expect(owner_api_key_count).to eq(1)
      expect(owner_api_key(key_id)["revoked_at"]).to be_nil
      expect(owner_audit_rows).to be_empty
    end

    it "lets admins list keys" do
      sign_in(admin)
      get "/admin/keys"

      expect(response).to have_http_status(:ok)
      expect(response.body).to include("existing")
    end
  end

  describe "create" do
    before { sign_in(admin) }

    it "shows the token once, in the response itself, and stores only its hash" do
      create_key

      expect(response).to have_http_status(:created)
      token = token_in(response.body)
      expect(token).to match(/\Arotten_[1-9][0-9]*_[A-Za-z0-9_-]{43}\z/)
      id, secret = token.delete_prefix("rotten_").split("_", 2)

      row = owner_api_key(Integer(id))
      expect(row).to include("name" => "worker-db1", "fqdn" => "db1.example.test", "created_by" => admin.email,
                             "revoked_at" => nil, "revoked_by" => nil)
      expect(row["secret_hash"]).to eq(OpenSSL::Digest::SHA256.hexdigest(secret))
      expect(response.body).not_to include(row["secret_hash"])
    end

    it "marks the response no-store and keeps the token out of the session and flash" do
      create_key
      token = token_in(response.body)

      expect(response.headers["cache-control"]).to include("no-store")
      expect(response).not_to be_redirect
      expect(flash.to_h.values.join).not_to include(token)
      expect(session.to_h.to_s).not_to include(token)

      get "/admin/keys"
      expect(response.body).to include("worker-db1")
      expect(response.body).not_to include(token)
      expect(response.body).not_to include(token.split("_", 3).last)
    end

    it "writes the creation to the audit log" do
      create_key
      id = Integer(token_in(response.body)[/\Arotten_(\d+)_/, 1])

      rows = owner_audit_rows
      expect(rows.size).to eq(1)
      expect(rows.first).to include("actor_user_id" => admin.id.to_s, "actor_email" => admin.email,
                                    "action" => "api_key.create", "target_type" => "api_key", "target_id" => id.to_s)
      expect(JSON.parse(rows.first["details"])).to eq("name" => "worker-db1", "fqdn" => "db1.example.test")
    end

    it "normalizes the fqdn the way the server compares it" do
      create_key(name: "  worker-db2 ", fqdn: " DB2.Example.Test. ")

      expect(response).to have_http_status(:created)
      id = Integer(token_in(response.body)[/\Arotten_(\d+)_/, 1])
      expect(owner_api_key(id)).to include("name" => "worker-db2", "fqdn" => "db2.example.test")
    end

    {
      "a blank name" => { name: " " },
      "a name that's too long" => { name: "a" * 65 },
      "a name with spaces" => { name: "two words" },
      "a name with markup" => { name: "<b>x</b>" },
      "a name starting with punctuation" => { name: "-worker" },
      "a blank fqdn" => { fqdn: "" },
      "an fqdn with a bad label" => { fqdn: "db_1.example.test" },
      "an fqdn with an empty label" => { fqdn: "db1..example.test" },
      "an fqdn label starting with a hyphen" => { fqdn: "-db1.example.test" },
      "an fqdn label that's too long" => { fqdn: "#{'a' * 64}.example.test" },
      "an fqdn that's too long" => { fqdn: (["a" * 63] * 4).join(".") },
      "an fqdn with a port" => { fqdn: "db1.example.test:5432" },
      "an fqdn with non-ASCII" => { fqdn: "dé1.example.test" }
    }.each do |what, attrs|
      it "refuses #{what} and writes nothing" do
        create_key(**{ name: "worker-db1", fqdn: "db1.example.test" }.merge(attrs))

        expect(response).to have_http_status(:unprocessable_content)
        expect(token_in(response.body)).to be_nil
        expect(owner_api_key_count).to eq(0)
        expect(owner_audit_rows).to be_empty
      end
    end

    it "accepts names and fqdns at the length limits" do
      create_key(name: "a" * 64, fqdn: (["a" * 63] * 3 + ["a" * 61]).join("."))

      expect(response).to have_http_status(:created)
    end

    it "refuses a name that's taken, even by a revoked key" do
      id = owner_create_api_key(name: "worker-db1")
      ApiKeyHelpers.with_owner do |conn|
        conn.exec_params("update rotten.api_keys set revoked_at = now(), revoked_by = 'cli' where id = $1", [id])
      end

      create_key(name: "worker-db1")

      expect(response).to have_http_status(:unprocessable_content)
      expect(response.body).to include("Name has already been taken")
      expect(owner_api_key_count).to eq(1)
      expect(owner_audit_rows).to be_empty
    end

    it "refuses a request without the api_key params" do
      post "/admin/keys", params: { name: "x" }

      expect(response).to have_http_status(:bad_request)
      expect(owner_api_key_count).to eq(0)
    end
  end

  describe "revoke" do
    before { sign_in(admin) }

    let!(:key_id) { owner_create_api_key(name: "to-revoke") }

    it "revokes the key, records who did it, and audits it" do
      post "/admin/keys/#{key_id}/revoke"

      expect(response).to redirect_to("/admin/keys")
      row = owner_api_key(key_id)
      expect(row["revoked_at"]).to be_present
      expect(row["revoked_by"]).to eq(admin.email)
      rows = owner_audit_rows
      expect(rows.size).to eq(1)
      expect(rows.first).to include("actor_user_id" => admin.id.to_s, "actor_email" => admin.email,
                                    "action" => "api_key.revoke", "target_type" => "api_key", "target_id" => key_id.to_s)
      expect(JSON.parse(rows.first["details"])).to eq("name" => "to-revoke", "fqdn" => "db.example.test")
      follow_redirect!
      expect(response.body).to include("Revoked to-revoke")
    end

    it "is idempotent: a second revoke changes nothing and doesn't fail" do
      post "/admin/keys/#{key_id}/revoke"
      first = owner_api_key(key_id)

      other = create_password_user(email: "keys.admin2@example.test", password: password, role: "admin")
      sign_in(other)
      post "/admin/keys/#{key_id}/revoke"

      expect(response).to redirect_to("/admin/keys")
      expect(owner_api_key(key_id).slice("revoked_at", "revoked_by")).to eq(first.slice("revoked_at", "revoked_by"))
      expect(owner_audit_rows.size).to eq(1)
      follow_redirect!
      expect(response.body).to include("to-revoke was already revoked")
    end

    it "404s an unknown or malformed key id" do
      post "/admin/keys/#{key_id + 1000}/revoke"
      expect(response).to have_http_status(:not_found)

      post "/admin/keys/abc/revoke"
      expect(response).to have_http_status(:not_found)

      expect(owner_audit_rows).to be_empty
    end
  end
end
