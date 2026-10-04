require "rails_helper"

RSpec.describe OidcLogin do
  let(:config) do
    RottenUi::OidcConfig.new(issuer: "https://idp.example.test", client_id: "c", client_secret: "s", groups_claim: "groups",
                             viewer_group: nil, admin_group: "rotten-admins", redirect_uri: nil)
  end

  let(:provider) { "openid_connect:https://idp.example.test" }

  def auth(uid: "sub-race", email: "race@example.test", groups: [])
    OmniAuth::AuthHash.new(provider: "openid_connect", uid: uid, info: { email: email, name: "Racer", email_verified: true },
                           extra: { raw_info: { "groups" => groups } })
  end

  before do
    User.delete_all
  end

  it "refuses to provision without a provider" do
    result = described_class.new(config: config, provider: nil).call(auth)

    expect(result.error).to eq(:invalid)
    expect(User.count).to eq(0)
  end

  it "recovers when a concurrent login creates the same user first" do
    racer = nil
    allow(User).to receive(:create!).and_wrap_original do |original, *args, **kwargs|
      # The racer commits on its own connection, the way a concurrent request would.
      racer ||= Thread.new do
        User.connection_pool.with_connection do |connection|
          connection.select_value(<<~SQL)
            insert into users (email, provider, provider_uid, role)
            values ('race@example.test', 'openid_connect:https://idp.example.test', 'sub-race', 'viewer') returning id
          SQL
        end
      end.value
      original.call(*args, **kwargs)
    end

    result = described_class.new(config: config, provider: provider).call(auth)

    expect(result.user).to be_present
    expect(result.user.id).to eq(racer)
    expect(User.count).to eq(1)
  end

  it "retries and resyncs when a concurrent login creates the same subject under another email" do
    racer = nil
    allow(User).to receive(:create!).and_wrap_original do |original, *args, **kwargs|
      racer ||= Thread.new do
        User.connection_pool.with_connection do |connection|
          connection.select_value(<<~SQL)
            insert into users (email, provider, provider_uid, role)
            values ('first@example.test', 'openid_connect:https://idp.example.test', 'sub-race', 'viewer') returning id
          SQL
        end
      end.value
      original.call(*args, **kwargs)
    end

    result = described_class.new(config: config, provider: provider).call(auth)

    expect(result.error).to be_nil
    expect(result.user.id).to eq(racer)
    expect(result.user.reload.email).to eq("race@example.test")
    expect(User.count).to eq(1)
    expect(User).to have_received(:create!).once
  end

  it "denies, rather than raising, when a resynced email collides with another user" do
    User.create!(email: "taken@example.test", role: "viewer")
    mine = User.create!(email: "mine@example.test", provider: provider, provider_uid: "sub-mine", role: "viewer")

    result = described_class.new(config: config, provider: provider).call(auth(uid: "sub-mine", email: "Taken@example.test"))

    expect(result.user).to be_nil
    expect(result.error).to eq(:conflict)
    expect(mine.reload.email).to eq("mine@example.test")
  end

  it "keeps identities from different providers apart" do
    User.create!(email: "fake@example.test", provider: "fake", provider_uid: "sub-shared", role: "viewer")

    result = described_class.new(config: config, provider: provider).call(auth(uid: "sub-shared", email: "real@example.test"))

    expect(result.user.email).to eq("real@example.test")
    expect(User.count).to eq(2)
  end

  it "caps very long values instead of storing them" do
    long = "g" * 1000
    result = described_class.new(config: config, provider: provider).call(auth(groups: [long, "ok"]))

    expect(result.user.groups).to eq(["ok"])
  end

  describe "auditing", :api_keys do
    let(:config) do
      RottenUi::OidcConfig.new(issuer: "https://idp.example.test", client_id: "c", client_secret: "s", groups_claim: "groups",
                               viewer_group: "rotten-viewers", admin_group: "rotten-admins", redirect_uri: nil)
    end

    def login(groups:, uid: "sub-audit", email: "audit@example.test")
      described_class.new(config: config, provider: provider).call(auth(uid: uid, email: email, groups: groups))
    end

    def known(role, active: true)
      groups = role == "admin" ? ["rotten-admins"] : ["rotten-viewers"]
      User.create!(email: "audit@example.test", provider: provider, provider_uid: "sub-audit", role: role, groups: groups,
                   active: active)
    end

    def audit_rows
      owner_audit_rows.map do |row|
        row.slice("actor_user_id", "actor_email", "action", "target_type", "target_id")
           .merge("details" => JSON.parse(row["details"]))
      end
    end

    def row(action, user, **details)
      { "actor_user_id" => nil, "actor_email" => "oidc", "action" => action, "target_type" => "user",
        "target_id" => user.id.to_s, "details" => { "email" => user.email, **details.transform_keys(&:to_s) } }
    end

    it "records a role change made by the IdP's groups" do
      user = known("viewer")

      expect(login(groups: ["rotten-admins"]).user).to eq(user)

      expect(audit_rows).to eq([row("user.role_change", user, from: "viewer", to: "admin")])
    end

    it "records nothing when the role stays the same, or for a new user" do
      user = known("admin")
      login(groups: ["rotten-admins"])
      login(groups: ["rotten-viewers"], uid: "sub-new", email: "new@example.test")

      expect(user.reload.role).to eq("admin")
      expect(User.count).to eq(2)
      expect(audit_rows).to be_empty
    end

    it "records an admin who lost every group losing admin and access" do
      user = known("admin")

      expect(login(groups: []).error).to eq(:not_authorized)

      expect(audit_rows).to eq([row("user.role_change", user, from: "admin", to: "viewer"),
                                row("user.access_lost", user)])
    end

    it "records lost access once, however many refused logins follow" do
      user = known("viewer")

      2.times { expect(login(groups: ["someone-else"]).error).to eq(:not_authorized) }

      expect(audit_rows).to eq([row("user.access_lost", user)])
    end

    it "records lost access again only after access came back" do
      user = known("viewer")

      login(groups: [])
      expect(login(groups: ["rotten-viewers"]).user).to eq(user)
      login(groups: [])

      expect(audit_rows).to eq([row("user.access_lost", user), row("user.access_lost", user)])
    end

    it "records no lost access for a user disabled before they lost their groups, but still ends sessions" do
      user = known("admin", active: false)

      expect { expect(login(groups: []).error).to eq(:not_authorized) }.to change { user.reload.session_generation }.by(1)

      expect(audit_rows).to eq([row("user.role_change", user, from: "admin", to: "viewer")])
    end

    it "records no lost access for a disabled user who is still in their groups" do
      known("viewer", active: false)

      expect(login(groups: ["rotten-viewers"]).error).to eq(:inactive)

      expect(audit_rows).to be_empty
    end

    it "decides the role from the groups it stores, so a group past the cap counts for nothing" do
      fillers = Array.new(OidcLogin::MAX_GROUPS) { |i| "filler-#{i}" }
      user = known("viewer")

      expect(login(groups: fillers + ["rotten-viewers"]).error).to eq(:not_authorized)
      expect(login(groups: fillers + ["rotten-viewers"]).error).to eq(:not_authorized)

      expect(user.reload.groups).to eq(fillers)
      expect(audit_rows).to eq([row("user.access_lost", user)])
    end

    it "locks the user row before deciding whether access was lost" do
      user = known("viewer")
      locked = []
      allow_any_instance_of(User).to receive(:lock!).and_wrap_original do |original, *args|
        locked << original.receiver.id
        original.call(*args)
      end
      allow(UiAuditLog).to receive(:record!).and_wrap_original do |original, **kwargs|
        expect(locked).to eq([user.id])
        original.call(**kwargs)
      end

      login(groups: [])

      expect(UiAuditLog).to have_received(:record!).once
    end

    it "on a conflict, resyncs all but the email and records the role change and lost access, once" do
      user = known("admin")
      User.create!(email: "taken@example.test", role: "viewer")

      2.times { expect(login(groups: [], email: "taken@example.test").error).to eq(:conflict) }

      expect(user.reload).to have_attributes(role: "viewer", groups: [], email: "audit@example.test")
      expect(audit_rows).to eq([row("user.role_change", user, from: "admin", to: "viewer"),
                                row("user.access_lost", user)])
    end

    describe "when the audit row can't be written" do
      before do
        allow(UiAuditLog).to receive(:record!).and_raise(ActiveRecord::StatementInvalid, "audit failed")
      end

      it "rolls back the role change" do
        user = known("viewer")

        expect { login(groups: ["rotten-admins"]) }.to raise_error(ActiveRecord::StatementInvalid, /audit failed/)

        expect(UiAuditLog).to have_received(:record!)
        expect(user.reload).to have_attributes(role: "viewer", groups: ["rotten-viewers"])
      end

      it "rolls back the resync of a user who lost access" do
        user = known("viewer")

        expect { login(groups: []) }.to raise_error(ActiveRecord::StatementInvalid, /audit failed/)

        expect(UiAuditLog).to have_received(:record!)
        expect(user.reload).to have_attributes(role: "viewer", groups: ["rotten-viewers"], session_generation: 0)
      end
    end
  end

  describe "email characters" do
    def login_with(email)
      described_class.new(config: config, provider: provider).call(auth(uid: "sub-#{email.hash}", email: email))
    end

    # ZWNJ and ZWJ are part of how Persian and Indic scripts are written.
    {
      "a zero-width non-joiner (Persian)" => "مهدی\u200Cرضایی@example.ir",
      "a zero-width joiner (Devanagari)" => "क्\u200Dष@example.in"
    }.each do |label, email|
      it "accepts an address with #{label}" do
        result = login_with(email)

        expect(result.error).to be_nil
        expect(result.user.email).to eq(email)
      end
    end

    {
      "zero-width space" => "\u200B",
      "word joiner" => "\u2060",
      "byte order mark" => "\uFEFF",
      "left-to-right embedding" => "\u202A",
      "right-to-left embedding" => "\u202B",
      "pop directional formatting" => "\u202C",
      "left-to-right override" => "\u202D",
      "right-to-left override" => "\u202E",
      "left-to-right isolate" => "\u2066",
      "right-to-left isolate" => "\u2067",
      "first strong isolate" => "\u2068",
      "pop directional isolate" => "\u2069",
      "left-to-right mark" => "\u200E",
      "right-to-left mark" => "\u200F",
      "Arabic letter mark" => "\u061C",
      "soft hyphen" => "\u00AD",
      "Mongolian vowel separator" => "\u180E",
      "invisible times" => "\u2062",
      "inhibit symmetric swapping" => "\u206A",
      "interlinear annotation anchor" => "\uFFF9",
      "language tag" => "\u{E0001}",
      "tag letter" => "\u{E0041}",
      "C0 control" => "\u0001",
      "C1 control" => "\u0085"
    }.each do |label, char|
      it "refuses an address containing a #{label}" do
        result = login_with("vic#{char}tim@example.test")

        expect(result).to have_attributes(user: nil, error: :missing_email)
        expect(User.count).to eq(0)
      end
    end
  end
end
