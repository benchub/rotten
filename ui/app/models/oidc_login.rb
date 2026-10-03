# Turns an OmniAuth auth hash into a provisioned user, or a reason to refuse.
#
# Rules:
# - Match on (provider, sub), then on email, then create a user. provider
#   carries the issuer, so a sub only ever matches users from the same issuer.
# - users.email is unique, so only an email the IdP says is verified
#   (email_verified true, or the string "true") is ever written. Creating or
#   linking a user needs one; a resync keeps the stored email otherwise.
# - An email match only links a user with no provider identity yet.
# - A user matched on (provider, sub) has name, email, groups and role resynced
#   on every login, even one that is then refused, so a user who leaves the
#   groups loses admin. A refused login never creates, links or changes any
#   other row, and never gets a session.
# - Role comes from groups and fails closed: admin needs the admin group;
#   otherwise viewer, if the viewer group is unset or the user is in it.
class OidcLogin
  Result = Data.define(:user, :error)

  MAX_GROUPS = 500
  MAX_GROUP_LENGTH = 256
  MAX_EMAIL_LENGTH = 320
  MAX_NAME_LENGTH = 256
  MAX_SUBJECT_LENGTH = 256
  EMAIL_FORMAT = /\A[^@\s]+@[^@\s]+\z/

  def initialize(config:, provider:)
    @config = config
    @provider = provider
  end

  def call(auth)
    return deny(:invalid) if provider.blank?

    claims = claims_from(auth)
    return deny(:invalid) unless claims[:sub]
    return deny(:missing_email) unless claims[:email]

    role = role_for(claims[:groups])
    attempts = 0
    begin
      attempts += 1
      provision(claims, role)
    rescue ActiveRecord::RecordNotUnique
      # Another login created this user, or the resynced email belongs to
      # someone else. Look again once; a second collision is a real conflict.
      retry if attempts < 2
      deny(:conflict)
    end
  end

  private

  attr_reader :config, :provider

  def provision(claims, role)
    User.transaction { provision_in_transaction(claims, role) }
  end

  def provision_in_transaction(claims, role)
    user = User.find_by(provider: provider, provider_uid: claims[:sub])
    return resync(user, claims, role) if user

    # Everything below would claim or create a row, so a login that's going to
    # be refused stops here and changes nothing. users.email is unique, so an
    # unverified email must never take it: that would lock out its real owner.
    return deny(:not_authorized) if role.nil?
    return deny(:unverified_email) unless claims[:email_verified]

    user = User.find_by(email: claims[:email])
    unless user
      return Result.new(user: User.create!(attributes(claims, role).merge(last_login_at: Time.current)), error: nil)
    end
    return deny(:conflict) unless linkable?(user)
    return deny(:inactive) unless user.active?

    user.update!(attributes(claims, role).merge(last_login_at: Time.current))
    Result.new(user: user, error: nil)
  end

  # The user matched on (issuer, sub), so this really is them: resync even if
  # the login is then refused, so someone who left the groups loses admin.
  # The email only changes when the IdP says the new one is verified.
  def resync(user, claims, role)
    attributes = attributes(claims, role || "viewer")
    attributes.delete(:email) unless claims[:email_verified]
    user.update!(attributes)
    return deny(:not_authorized) if role.nil?
    return deny(:inactive) unless user.active?

    user.update!(last_login_at: Time.current)
    Result.new(user: user, error: nil)
  end

  def attributes(claims, role)
    {
      provider: provider,
      provider_uid: claims[:sub],
      email: claims[:email],
      name: claims[:name],
      groups: claims[:groups].first(MAX_GROUPS),
      role: role
    }
  end

  # Linking hands an existing row to this identity, so it needs a row with no
  # identity yet. The caller has already required a verified email.
  def linkable?(user)
    user.provider_uid.blank?
  end

  def role_for(groups)
    return "admin" if config.admin_group && groups.include?(config.admin_group)
    return "viewer" if config.viewer_group.nil? || groups.include?(config.viewer_group)

    nil
  end

  def deny(error)
    Result.new(user: nil, error: error)
  end

  def claims_from(auth)
    info = fetch(auth, "info")
    raw_info = fetch(fetch(auth, "extra"), "raw_info")
    email = clean_string(fetch(info, "email"), MAX_EMAIL_LENGTH)
    verified = fetch(info, "email_verified")
    verified = fetch(raw_info, "email_verified") if verified.nil?

    {
      sub: clean_string(subject(fetch(auth, "uid")), MAX_SUBJECT_LENGTH),
      email: email&.match?(EMAIL_FORMAT) ? email.downcase : nil,
      email_verified: [true, "true"].include?(verified),
      name: clean_string(fetch(info, "name"), MAX_NAME_LENGTH),
      groups: groups_from(fetch(raw_info, config.groups_claim))
    }
  end

  def subject(value)
    value.is_a?(Integer) ? value.to_s : value
  end

  # The claim may be missing, a single string, or an array. Anything else, and
  # any element that isn't a usable string, counts as no group.
  def groups_from(value)
    values = case value
    when String then [value]
    when Array then value
    else []
    end

    values.filter_map { |group| clean_string(group, MAX_GROUP_LENGTH, strip: false) }.uniq
  end

  def clean_string(value, max_length, strip: true)
    return nil unless value.is_a?(String) && value.valid_encoding? && !value.include?("\0")

    value = value.strip if strip
    return nil if value.empty? || value.length > max_length

    value
  end

  def fetch(hash, key)
    return nil unless hash.is_a?(Hash)

    hash.key?(key.to_s) ? hash[key.to_s] : hash[key.to_sym]
  end
end
