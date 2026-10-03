require "rails_helper"

# Feeds the OIDC callback claims an identity provider shouldn't send but
# might: wrong types, bad encodings, control and invisible characters, huge
# values, SQL and HTML metacharacters, and lookalikes of real emails and
# groups. Every login must end in a redirect home or a 403, never a 500; a
# refusal leaves no session and no rows; and whatever is stored is clean text.
# A fixed seed list keeps it repeatable.
module ProvisioningFuzz
  STRINGS = {
    "empty" => "",
    "blank" => " \t ",
    "huge" => "a" * 100_000,
    "NUL byte" => "a\0b",
    "control characters" => "a\u0007b\u001Bc",
    "CRLF header" => "a\r\nSet-Cookie: x=1",
    "invalid UTF-8" => (+"caf\xC3(").force_encoding(Encoding::UTF_8),
    "binary with high bytes" => (+"caf\xE9").force_encoding(Encoding::BINARY),
    "UTF-16" => "abc".encode(Encoding::UTF_16LE),
    "RTL override" => "\u202Etxt.exe",
    "zero-width space" => "ad\u200Bmin",
    "soft hyphen" => "ad\u00ADmin",
    "tag characters" => "admin\u{E0041}\u{E007F}",
    "Cyrillic homoglyphs" => "\u0430dmin",
    "SQL metacharacters" => "x'); drop table users; --",
    "HTML" => "<script>alert(1)</script>",
    "emoji" => "\u{1F600}" * 10
  }.freeze

  VALUES = STRINGS.merge(
    "nil" => nil,
    "integer" => 42,
    "bignum" => 2**200,
    "negative" => -1,
    "float" => 1.5,
    "true" => true,
    "false" => false,
    "symbol" => :admin,
    "array" => %w[a b],
    "empty array" => [],
    "hash" => { "a" => "b" },
    "nested hash" => { "a" => { "b" => ["c"] } }
  ).freeze

  VICTIM = "victim@example.test".freeze

  # Format characters render as nothing, so they make an address look like
  # another. ZWNJ and ZWJ aren't here: real Persian and Indic addresses use them.
  INVISIBLE = /[\p{Cf}&&[^\u200C\u200D]]/

  # Each must neither create a user under that email nor take over VICTIM.
  EMAILS = {
    "CRLF injection" => "#{VICTIM}\r\nBcc: attacker@example.test",
    "trailing NUL" => "#{VICTIM}\0",
    "embedded NUL" => "vic\0tim@example.test",
    "SQL quote" => "o'brien@example.test",
    "SQL tautology" => "x'or'1'='1@example.test",
    "SQL comment" => "#{VICTIM}'--",
    "Cyrillic homoglyph" => "v\u0456ctim@example.test",
    "fullwidth" => "\uFF56\uFF49\uFF43\uFF54\uFF49\uFF4D@example.test",
    "RTL override" => "\u202E#{VICTIM}",
    "zero-width space" => "vic\u200Btim@example.test",
    "word joiner" => "vic\u2060tim@example.test",
    "byte order mark" => "\uFEFF#{VICTIM}",
    "left-to-right mark" => "#{VICTIM}\u200E",
    "Arabic letter mark" => "vic\u061Ctim@example.test",
    "bidi isolate" => "\u2067#{VICTIM}\u2069",
    "C1 control" => "vic\u0085tim@example.test",
    "soft hyphen" => "vic\u00ADtim@example.test",
    "tag characters" => "victim\u{E0041}\u{E0020}\u{E007F}@example.test",
    "invisible operator" => "vic\u2061tim@example.test",
    "no-break space" => "vic\u00A0tim@example.test",
    "two @" => "attacker@#{VICTIM}",
    "HTML" => "<script>alert(1)</script>@example.test",
    "huge" => "#{'a' * 400}@example.test",
    "binary" => (+"vict\xEDm@example.test").force_encoding(Encoding::BINARY)
  }.freeze

  # Each must not grant admin.
  ADMIN_GROUP_LOOKALIKES = {
    "Cyrillic homoglyph" => "r\u043Etten-admins",
    "uppercase" => "ROTTEN-ADMINS",
    "trailing NUL" => "rotten-admins\0",
    "trailing newline" => "rotten-admins\n",
    "zero-width space" => "rotten-admins\u200B",
    "leading space" => " rotten-admins",
    "hash keyed by the group" => { "rotten-admins" => true },
    "nested array" => [["rotten-admins"]],
    "array of hashes" => [{ "name" => "rotten-admins" }],
    "comma-joined" => "rotten-viewers,rotten-admins",
    "symbol" => [:"rotten-admins"]
  }.freeze
end

RSpec.describe "OIDC provisioning with hostile claims", type: :request do
  let(:limits) do
    { email: OidcLogin::MAX_EMAIL_LENGTH, name: OidcLogin::MAX_NAME_LENGTH,
      provider_uid: OidcLogin::MAX_SUBJECT_LENGTH, provider: 2048 }
  end

  before do
    User.delete_all
    use_oidc_mode(viewer_group: "rotten-viewers", admin_group: "rotten-admins")
    @victim = create_password_user(email: ProvisioningFuzz::VICTIM, password: "victim-password-123", role: "admin")
    @victim_attributes = @victim.reload.attributes
  end

  def base_auth
    {
      "provider" => "openid_connect",
      "uid" => "sub-fuzz",
      "info" => { "email" => "fuzz@example.test", "name" => "Fuzz", "email_verified" => true },
      "extra" => { "raw_info" => { "sub" => "sub-fuzz", "email_verified" => true, "groups" => ["rotten-viewers"] } }
    }
  end

  # Signs in with the auth hash as the IdP sent it and returns the response.
  def sign_in_with(auth)
    OmniAuth.config.mock_auth[:openid_connect] = auth
    post "/auth/openid_connect"
    get "/auth/openid_connect/callback"
    response
  end

  def signed_in?
    get "/"
    response.status == 200
  end

  def expect_clean_rows
    User.find_each do |user|
      %i[email name provider provider_uid].each do |column|
        value = user[column]
        next if value.nil?

        expect(value.encoding).to eq(Encoding::UTF_8), "#{column} is #{value.encoding}"
        expect(value).to be_valid_encoding
        expect(value).not_to match(/\p{Cc}/), "#{column} has a control character: #{value.inspect}"
        expect(value.length).to be <= limits.fetch(column)
      end
      expect(user.email).to match(OidcLogin::EMAIL_FORMAT)
      expect(user.email).to eq(user.email.downcase)
      expect(user.email).not_to match(ProvisioningFuzz::INVISIBLE), "email has an invisible character: #{user.email.inspect}"
      expect(user.email).not_to match(/[[:space:]]/), "email has a space: #{user.email.inspect}"
      expect(User::ROLES).to include(user.role)
      expect(user.groups.size).to be <= OidcLogin::MAX_GROUPS
      user.groups.each do |group|
        expect(group).to be_valid_encoding
        expect(group).not_to match(/\p{Cc}/)
        expect(group.length).to be <= OidcLogin::MAX_GROUP_LENGTH
      end
    end
  end

  def expect_victim_untouched
    expect(User.find(@victim.id).attributes).to eq(@victim_attributes)
  end

  # The outcome of any login: home, signed in, with clean rows; or a 403 with
  # no session and nothing new stored.
  def expect_safe_outcome(auth)
    users_before = User.count
    status = sign_in_with(auth).status

    expect([302, 403]).to include(status), "got #{status}"
    if status == 302
      expect(response.location).to eq("http://www.example.com/")
      expect(signed_in?).to be(true)
      expect(response.body).not_to include("<script>alert(1)</script>")
    else
      expect(signed_in?).to be(false)
      expect(User.count).to eq(users_before)
    end
    expect_clean_rows
    expect_victim_untouched
  end

  def with_claim(path, value)
    auth = base_auth
    *parents, key = path
    target = parents.reduce(auth) { |hash, parent| hash[parent] }
    target[key] = value
    auth
  end

  {
    "uid" => ["uid"],
    "email" => %w[info email],
    "name" => %w[info name],
    "email_verified" => %w[info email_verified],
    "groups" => %w[extra raw_info groups],
    "a group" => %w[extra raw_info groups],
    "info" => ["info"],
    "extra" => ["extra"],
    "raw_info" => %w[extra raw_info]
  }.each do |claim, path|
    ProvisioningFuzz::VALUES.each do |label, value|
      it "survives #{claim} set to #{label}" do
        value = ["rotten-viewers", value] if claim == "a group"
        expect_safe_outcome(with_claim(path, value))
      end
    end
  end

  ProvisioningFuzz::VALUES.each do |label, value|
    next if value.nil? || value == false || value.is_a?(Symbol)

    it "survives the whole auth hash being #{label}" do
      expect_safe_outcome(value)
    end
  end

  ProvisioningFuzz::EMAILS.each do |label, email|
    it "doesn't let an email with #{label} take over or shadow #{ProvisioningFuzz::VICTIM}" do
      expect_safe_outcome(with_claim(%w[info email], email))
      expect(User.where(email: ProvisioningFuzz::VICTIM).count).to eq(1)
    end
  end

  ProvisioningFuzz::ADMIN_GROUP_LOOKALIKES.each do |label, group|
    it "doesn't grant admin for a group that is the admin group's #{label}" do
      expect_safe_outcome(with_claim(%w[extra raw_info groups], ["rotten-viewers", group]))
      expect(User.where(role: "admin").pluck(:id)).to eq([@victim.id])
    end
  end

  it "signs in the baseline, so the cases above aren't all refusals" do
    expect_safe_outcome(base_auth)
    expect(User.find_by(provider_uid: "sub-fuzz")).to have_attributes(email: "fuzz@example.test", role: "viewer")
  end

  describe "OidcLogin, called directly" do
    let(:login) { OidcLogin.new(config: Rails.configuration.x.oidc, provider: oidc_provider) }

    [nil, "auth", 42, [], {}, { info: nil }, OmniAuth::AuthHash.new].each do |auth|
      it "refuses #{auth.inspect} without raising" do
        expect(login.call(auth)).to have_attributes(user: nil, error: :invalid)
        expect(User.count).to eq(1)
      end
    end
  end
end
