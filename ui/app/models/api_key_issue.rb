# The new pass key form. On save it makes a secret, stores only its hash,
# audits the creation, and returns the token, the only copy of the secret.
class ApiKeyIssue
  include ActiveModel::Model
  include ActiveModel::Attributes

  NAME_MAX = 64
  NAME_FORMAT = /\A[A-Za-z0-9][A-Za-z0-9._-]*\z/
  # A host name as the server compares it: lowercase, no trailing dot, labels
  # of letters, digits and inner hyphens, 253 characters at most.
  FQDN_MAX = 253
  FQDN_LABEL = /[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?/
  FQDN_FORMAT = /\A#{FQDN_LABEL}(?:\.#{FQDN_LABEL})*\z/

  Issued = Data.define(:api_key, :token)

  attribute :name, :string
  attribute :fqdn, :string

  validates :name, presence: true, length: { maximum: NAME_MAX }
  validates :name, format: { with: NAME_FORMAT, message: "may only contain letters, digits, '.', '_' and '-', and must start with a letter or digit" },
                   allow_blank: true
  validates :fqdn, presence: true, length: { maximum: FQDN_MAX }
  validates :fqdn, format: { with: FQDN_FORMAT, message: "must be a host name such as db1.example.com" }, allow_blank: true
  validate :name_unused

  def self.human_attribute_name(attribute, options = {})
    attribute.to_s == "fqdn" ? "FQDN" : super
  end

  def name=(value)
    super(value.to_s.strip)
  end

  # The server lowercases and drops a trailing dot before comparing.
  def fqdn=(value)
    super(value.to_s.strip.downcase.delete_suffix("."))
  end

  # Returns an Issued, or nil with errors set.
  def save(actor)
    return nil unless valid?

    secret = RottenUi::PassKey.generate_secret
    ApiKey.transaction do
      id = ApiKey.insert_key!(name: name, secret_hash: RottenUi::PassKey.hash_secret(secret), fqdn: fqdn,
                              created_by: actor.email)
      UiAuditLog.record!(actor: actor, action: "api_key.create", target_type: ApiKey::AUDIT_TYPE, target_id: id,
                         details: { name: name, fqdn: fqdn })
      Issued.new(api_key: ApiKey.find(id), token: RottenUi::PassKey.token(id, secret))
    end
  rescue ActiveRecord::RecordNotUnique
    # Another admin took the name between the check and the insert.
    errors.add(:name, :taken, message: "has already been taken")
    nil
  end

  private

  def name_unused
    errors.add(:name, :taken, message: "has already been taken") if name.present? && ApiKey.exists?(name: name)
  end
end
