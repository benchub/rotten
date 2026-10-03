require "rails_helper"

RSpec.describe "OIDC login", type: :request do
  before do
    User.delete_all
  end

  context "in oidc mode" do
    before { use_oidc_mode(viewer_group: "rotten-viewers", admin_group: "rotten-admins") }

    it "creates a user and a session on the first callback" do
      mock_oidc_auth(uid: "sub-new", email: "New.Person@Example.test", name: "New Person", groups: ["rotten-viewers"])

      oidc_sign_in

      expect(response).to redirect_to("/")
      user = User.sole
      expect(user).to have_attributes(
        provider: oidc_provider,
        provider_uid: "sub-new",
        email: "new.person@example.test",
        name: "New Person",
        groups: ["rotten-viewers"],
        role: "viewer",
        active: true
      )
      expect(user.last_login_at).to be_within(1.minute).of(Time.current)
      expect(session[:user_id]).to eq(user.id)

      get "/"
      expect(response).to have_http_status(:ok)
      expect(response.body).to include("new.person@example.test")
    end

    it "resyncs name, email, groups and role on every login, including dropping admin" do
      mock_oidc_auth(uid: "sub-admin", email: "boss@example.test", name: "Boss", groups: %w[rotten-viewers rotten-admins])
      oidc_sign_in
      user = User.sole
      expect(user.role).to eq("admin")
      get "/admin"
      expect(response).to have_http_status(:ok)

      delete "/logout"
      mock_oidc_auth(uid: "sub-admin", email: "Former.Boss@example.test", name: "Former Boss", groups: ["rotten-viewers"])
      oidc_sign_in

      expect(User.count).to eq(1)
      expect(user.reload).to have_attributes(
        email: "former.boss@example.test",
        name: "Former Boss",
        groups: ["rotten-viewers"],
        role: "viewer"
      )
      get "/admin"
      expect(response).to have_http_status(:forbidden)
    end

    it "promotes a viewer to admin when the admin group appears" do
      mock_oidc_auth(uid: "sub-promote", groups: ["rotten-viewers"])
      oidc_sign_in
      expect(User.sole.role).to eq("viewer")

      mock_oidc_auth(uid: "sub-promote", groups: ["rotten-admins"])
      oidc_sign_in

      expect(User.sole.role).to eq("admin")
    end

    it "matches an existing unlinked user by email and links the subject" do
      existing = User.create!(email: "linked@example.test", name: "Old Name", role: "admin")
      mock_oidc_auth(uid: "sub-link", email: "LINKED@example.test", name: "Linked", groups: ["rotten-viewers"])

      oidc_sign_in

      expect(response).to redirect_to("/")
      expect(User.sole.id).to eq(existing.id)
      expect(existing.reload).to have_attributes(provider: oidc_provider, provider_uid: "sub-link", name: "Linked", role: "viewer")
      expect(session[:user_id]).to eq(existing.id)
    end

    it "matches on sub before email" do
      by_sub = User.create!(email: "sub-owner@example.test", provider: oidc_provider, provider_uid: "sub-first", role: "viewer")
      mock_oidc_auth(uid: "sub-first", email: "sub-owner@example.test", groups: ["rotten-viewers"])

      oidc_sign_in

      expect(session[:user_id]).to eq(by_sub.id)
      expect(User.count).to eq(1)
    end

    it "refuses to link an email that already belongs to a different subject" do
      other = User.create!(email: "taken@example.test", provider: oidc_provider, provider_uid: "sub-original", role: "admin")
      mock_oidc_auth(uid: "sub-impostor", email: "taken@example.test", groups: %w[rotten-viewers rotten-admins])

      oidc_sign_in

      expect(response).to have_http_status(:forbidden)
      expect(body_text).to include("couldn't be matched")
      expect(session[:user_id]).to be_nil
      expect(other.reload.provider_uid).to eq("sub-original")
    end

    it "refuses to link by email when the IdP says the email is unverified" do
      existing = User.create!(email: "unverified@example.test", role: "viewer")
      mock_oidc_auth(uid: "sub-unverified", email: "unverified@example.test", email_verified: false, groups: ["rotten-viewers"])

      expect { oidc_sign_in }.not_to(change { existing.reload.attributes })

      expect(response).to have_http_status(:forbidden)
      expect(body_text).to include("hasn't verified your email address")
      expect(session[:user_id]).to be_nil
      expect(User.count).to eq(1)
    end

    it "refuses to link by email when the IdP doesn't say whether the email is verified" do
      existing = User.create!(email: "unknown@example.test", role: "viewer")
      mock_oidc_auth(uid: "sub-unknown", email: "unknown@example.test", email_verified: :absent, groups: ["rotten-viewers"])

      expect { oidc_sign_in }.not_to(change { existing.reload.attributes })

      expect(response).to have_http_status(:forbidden)
      expect(body_text).to include("hasn't verified your email address")
      expect(session[:user_id]).to be_nil
      expect(User.count).to eq(1)
    end

    ["yes", 1, "1", "True"].each do |value|
      it "refuses to link by email when email_verified is #{value.inspect}, not true" do
        existing = User.create!(email: "odd@example.test", role: "viewer")
        mock_oidc_auth(uid: "sub-odd", email: "odd@example.test", email_verified: value, groups: ["rotten-viewers"])

        expect { oidc_sign_in }.not_to(change { existing.reload.attributes })

        expect(response).to have_http_status(:forbidden)
      end
    end

    it "links by email when email_verified is the string \"true\"" do
      existing = User.create!(email: "stringy@example.test", role: "viewer")
      mock_oidc_auth(uid: "sub-stringy", email: "stringy@example.test", email_verified: "true", groups: ["rotten-viewers"])

      oidc_sign_in

      expect(session[:user_id]).to eq(existing.id)
      expect(existing.reload.provider_uid).to eq("sub-stringy")
    end

    [false, :absent, "yes"].each do |value|
      it "refuses to create a user when email_verified is #{value.inspect}, and writes no row" do
        mock_oidc_auth(uid: "sub-fresh", email: "fresh@example.test", email_verified: value, groups: ["rotten-viewers"])

        oidc_sign_in

        expect(response).to have_http_status(:forbidden)
        expect(body_text).to include("hasn't verified your email address")
        expect(session[:user_id]).to be_nil
        expect(User.count).to eq(0)
      end
    end

    it "keeps the stored email when a matched user's new email is unverified, and still resyncs the rest" do
      user = User.create!(email: "victim-safe@example.test", name: "Old Name", provider: oidc_provider,
                          provider_uid: "sub-resync", role: "admin", groups: ["rotten-admins"])
      mock_oidc_auth(uid: "sub-resync", email: "victim@example.test", name: "New Name", email_verified: false,
                     groups: ["rotten-viewers"])

      oidc_sign_in

      expect(response).to redirect_to("/")
      expect(session[:user_id]).to eq(user.id)
      expect(user.reload).to have_attributes(email: "victim-safe@example.test", name: "New Name",
                                             groups: ["rotten-viewers"], role: "viewer")
    end

    it "keeps the stored email on a refused resync when the new email is unverified" do
      user = User.create!(email: "leaver-safe@example.test", provider: oidc_provider, provider_uid: "sub-unv-leaver",
                          role: "admin", groups: ["rotten-admins"])
      mock_oidc_auth(uid: "sub-unv-leaver", email: "victim@example.test", email_verified: :absent, groups: [])

      oidc_sign_in

      expect(response).to have_http_status(:forbidden)
      expect(user.reload).to have_attributes(email: "leaver-safe@example.test", role: "viewer", groups: [])
    end

    it "does not let an unverified email on a matched user lock out the email's real owner" do
      User.create!(email: "attacker@example.test", provider: oidc_provider, provider_uid: "sub-attacker2", role: "viewer")
      mock_oidc_auth(uid: "sub-attacker2", email: "owner@example.test", email_verified: false, groups: ["rotten-viewers"])
      oidc_sign_in
      delete "/logout"

      mock_oidc_auth(uid: "sub-owner", email: "owner@example.test", groups: ["rotten-viewers"])
      oidc_sign_in

      expect(response).to redirect_to("/")
      expect(User.find(session[:user_id])).to have_attributes(provider_uid: "sub-owner", email: "owner@example.test")
    end

    it "leaves an unlinked admin row completely unchanged when a refused login matches it by email" do
      admin = User.create!(email: "real-admin@example.test", name: "Real Admin", role: "admin", groups: ["rotten-admins"])
      mock_oidc_auth(uid: "sub-attacker", email: "real-admin@example.test", name: "Attacker", groups: ["some-other-group"])

      expect { oidc_sign_in }.not_to(change { admin.reload.attributes })

      expect(response).to have_http_status(:forbidden)
      expect(session[:user_id]).to be_nil
      expect(User.count).to eq(1)
    end

    it "does not let a different issuer take over a row with the same sub" do
      old = User.create!(email: "carried@example.test", provider: oidc_provider("https://old-idp.example.test"),
                         provider_uid: "sub-shared", role: "admin", groups: ["rotten-admins"])
      mock_oidc_auth(uid: "sub-shared", email: "carried@example.test", groups: %w[rotten-viewers rotten-admins])

      expect { oidc_sign_in }.not_to(change { old.reload.attributes })

      expect(response).to have_http_status(:forbidden)
      expect(body_text).to include("couldn't be matched")
      expect(session[:user_id]).to be_nil
      expect(User.count).to eq(1)
    end

    it "keeps users with the same sub from different issuers apart" do
      User.create!(email: "elsewhere@example.test", provider: oidc_provider("https://old-idp.example.test"),
                   provider_uid: "sub-twin", role: "admin")
      mock_oidc_auth(uid: "sub-twin", email: "here@example.test", groups: ["rotten-viewers"])

      oidc_sign_in

      expect(response).to redirect_to("/")
      expect(User.find(session[:user_id])).to have_attributes(provider: oidc_provider, email: "here@example.test", role: "viewer")
      expect(User.count).to eq(2)
    end

    it "stores the issuer in provider" do
      use_oidc_mode(viewer_group: "rotten-viewers", issuer: "https://other-idp.example.test/oauth2/default")
      mock_oidc_auth(uid: "sub-issuer", groups: ["rotten-viewers"])

      oidc_sign_in

      expect(User.sole.provider).to eq("openid_connect:https://other-idp.example.test/oauth2/default")
    end

    it "returns 403 and creates no session or user for someone in neither group" do
      mock_oidc_auth(uid: "sub-outsider", email: "outsider@example.test", groups: ["some-other-group"])

      oidc_sign_in

      expect(response).to have_http_status(:forbidden)
      expect(body_text).to include("not in a group that can use Rotten")
      expect(session[:user_id]).to be_nil
      expect(User.count).to eq(0)
      get "/"
      expect(response).to redirect_to("/login")
    end

    it "demotes an existing user who has left both groups, and still grants no session" do
      user = User.create!(email: "leaver@example.test", provider: oidc_provider, provider_uid: "sub-leaver",
                          role: "admin", groups: ["rotten-admins"])
      mock_oidc_auth(uid: "sub-leaver", email: "leaver@example.test", groups: [])

      oidc_sign_in

      expect(response).to have_http_status(:forbidden)
      expect(session[:user_id]).to be_nil
      expect(user.reload).to have_attributes(role: "viewer", groups: [])
    end

    it "ends an existing session when a later login is denied" do
      mock_oidc_auth(uid: "sub-session", groups: ["rotten-viewers"])
      oidc_sign_in
      expect(session[:user_id]).to be_present

      mock_oidc_auth(uid: "sub-session", groups: [])
      oidc_sign_in

      expect(response).to have_http_status(:forbidden)
      expect(session[:user_id]).to be_nil
    end

    it "lets an admin-group member in even without the viewer group" do
      mock_oidc_auth(uid: "sub-admin-only", groups: ["rotten-admins"])

      oidc_sign_in

      expect(User.sole.role).to eq("admin")
      expect(session[:user_id]).to eq(User.sole.id)
    end

    it "refuses an inactive user and grants no session" do
      user = User.create!(email: "off@example.test", provider: oidc_provider, provider_uid: "sub-off", role: "viewer", active: false)
      mock_oidc_auth(uid: "sub-off", email: "off@example.test", groups: ["rotten-viewers"])

      oidc_sign_in

      expect(response).to have_http_status(:forbidden)
      expect(response.body).to include("disabled")
      expect(session[:user_id]).to be_nil
      expect(user.reload.active).to be(false)
    end

    it "denies a login with no email, with a clear message and no user" do
      mock_oidc_auth(uid: "sub-no-email", email: nil, groups: ["rotten-viewers"])

      oidc_sign_in

      expect(response).to have_http_status(:forbidden)
      expect(body_text).to include("didn't share an email address")
      expect(session[:user_id]).to be_nil
      expect(User.count).to eq(0)
    end

    it "denies a login with no subject" do
      mock_oidc_auth(uid: nil, groups: ["rotten-viewers"])

      oidc_sign_in

      expect(response).to have_http_status(:forbidden)
      expect(session[:user_id]).to be_nil
      expect(User.count).to eq(0)
    end

    it "resets the session on login to prevent fixation" do
      other = User.create!(email: "earlier@example.test", role: "viewer")
      post "/__test/sign_in", params: { user_id: other.id }
      before_id = session.id.to_s
      expect(before_id).to be_present
      mock_oidc_auth(uid: "sub-fixation", groups: ["rotten-viewers"])

      oidc_sign_in

      expect(session[:user_id]).to eq(User.find_by!(provider_uid: "sub-fixation").id)
      expect(session.id.to_s).not_to eq(before_id)
    end

    it "reads groups from the configured claim" do
      use_oidc_mode(viewer_group: "rotten-viewers", admin_group: "rotten-admins", groups_claim: "roles")
      mock_oidc_auth(uid: "sub-claim", claim: "roles", groups: ["rotten-admins"],
                     raw_info: { "groups" => ["rotten-viewers"] })

      oidc_sign_in

      expect(User.sole).to have_attributes(role: "admin", groups: ["rotten-admins"])
    end

    it "accepts a single group sent as a string" do
      mock_oidc_auth(uid: "sub-string", groups: "rotten-admins")

      oidc_sign_in

      expect(User.sole).to have_attributes(role: "admin", groups: ["rotten-admins"])
    end

    [
      ["a hash", { "rotten-admins" => true }],
      ["an integer", 42],
      ["true", true],
      ["null", nil],
      ["an array of non-strings", [1, { "name" => "rotten-admins" }, ["rotten-admins"], nil]]
    ].each do |label, value|
      it "treats a groups claim that is #{label} as no groups without crashing" do
        mock_oidc_auth(uid: "sub-nasty", groups: value)

        oidc_sign_in

        expect(response).to have_http_status(:forbidden)
        expect(session[:user_id]).to be_nil
      end
    end

    it "drops group names with NUL bytes or invalid encoding instead of crashing" do
      mock_oidc_auth(uid: "sub-bytes", groups: ["rotten-viewers", "bad\u0000group", "\xFF".dup.force_encoding("UTF-8")])

      oidc_sign_in

      expect(response).to redirect_to("/")
      expect(User.sole.groups).to eq(["rotten-viewers"])
    end
  end

  context "in oidc mode with no viewer group" do
    before { use_oidc_mode(admin_group: "rotten-admins") }

    it "makes any authenticated user a viewer" do
      mock_oidc_auth(uid: "sub-anyone", groups: ["unrelated"])

      oidc_sign_in

      expect(User.sole.role).to eq("viewer")
      expect(session[:user_id]).to eq(User.sole.id)
    end

    it "gives a viewer at most, never an admin, when the groups claim is missing" do
      User.create!(email: "was-admin@example.test", provider: oidc_provider, provider_uid: "sub-missing", role: "admin")
      mock_oidc_auth(uid: "sub-missing", email: "was-admin@example.test", groups: :absent)

      oidc_sign_in

      expect(response).to redirect_to("/")
      expect(User.sole).to have_attributes(role: "viewer", groups: [])
    end
  end

  context "in oidc mode with a viewer group set and the groups claim missing" do
    before { use_oidc_mode(viewer_group: "rotten-viewers", admin_group: "rotten-admins") }

    it "denies the login" do
      mock_oidc_auth(uid: "sub-missing-claim", groups: :absent)

      oidc_sign_in

      expect(response).to have_http_status(:forbidden)
      expect(session[:user_id]).to be_nil
    end
  end

  context "in oidc mode with no admin group" do
    before { use_oidc_mode }

    it "makes nobody an admin" do
      mock_oidc_auth(uid: "sub-no-admin", groups: %w[rotten-admins admins admin])

      oidc_sign_in

      expect(User.sole.role).to eq("viewer")
    end
  end

  context "when OmniAuth reports a failure" do
    before { use_oidc_mode }

    it "shows a friendly error without leaking details" do
      OmniAuth.config.mock_auth[:openid_connect] = :invalid_credentials

      oidc_sign_in
      expect(response).to redirect_to("/auth/failure")
      follow_redirect!
      expect(response).to redirect_to("/login")
      follow_redirect!

      expect(body_text).to include("Sign-in didn't work")
      expect(response.body).not_to include("invalid_credentials")
      expect(session[:user_id]).to be_nil
    end

    it "ignores a message param on the failure page" do
      get "/auth/failure", params: { message: "<script>alert(1)</script>secret detail", strategy: "openid_connect" }
      follow_redirect!

      expect(response.body).not_to include("secret detail")
      expect(response.body).not_to include("<script>alert(1)")
    end
  end

  context "in password mode" do
    it "does not accept OIDC callbacks" do
      mock_oidc_auth(uid: "sub-password-mode", groups: ["rotten-viewers"])

      post "/auth/openid_connect"
      get "/auth/openid_connect/callback"

      expect(response).to have_http_status(:not_found)
      expect(User.count).to eq(0)
    end
  end

  describe "the login page" do
    it "shows a Sign in button that POSTs to /auth/openid_connect in oidc mode" do
      use_oidc_mode

      get "/login"

      form = Nokogiri::HTML(response.body).at_css("form[action='/auth/openid_connect']")
      expect(form).to be_present
      expect(form["method"]).to eq("post")
      expect(form.at_css("button").text).to eq("Sign in")
    end

    it "shows no OIDC button in password mode" do
      get "/login"

      expect(response.body).not_to include("/auth/openid_connect")
    end
  end

  it "does not route the fake login outside development" do
    post "/auth/fake/admin"

    expect(response).to have_http_status(:not_found)
  end
end
