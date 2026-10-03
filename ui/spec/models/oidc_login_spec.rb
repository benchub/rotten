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
