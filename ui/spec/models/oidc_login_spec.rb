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
end
