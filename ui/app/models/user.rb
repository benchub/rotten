class User < ApplicationRecord
  ROLES = %w[viewer admin].freeze
  # users.provider for accounts that sign in with a password. OIDC users have
  # openid_connect:<issuer> instead, and never a usable password.
  PASSWORD_PROVIDER = "password".freeze
  MAX_PASSWORD_BYTES = ActiveModel::SecurePassword::MAX_PASSWORD_LENGTH_ALLOWED

  # OIDC users have no password, so the built-in validations, which demand
  # one, are off. Password users get the equivalent checks below.
  has_secure_password validations: false

  normalizes :email, with: ->(email) { email.strip.downcase }

  validates :email, presence: true
  validates :role, inclusion: { in: ROLES }
  validate :password_usable

  # The user for a password login, or nil. Only active password users can
  # match. Every other case, unknown email included, still costs one bcrypt
  # hash (authenticate_by hashes against a throwaway record when nothing
  # matches), so response time doesn't reveal which emails exist.
  def self.authenticate_password_login(email:, password:)
    return nil unless email.is_a?(String) && password.is_a?(String)
    return nil if password.bytesize > MAX_PASSWORD_BYTES

    where(provider: PASSWORD_PROVIDER, active: true).authenticate_by(email: email, password: password)
  end

  def admin?
    role == "admin"
  end

  def password_login?
    provider == PASSWORD_PROVIDER
  end

  # An HMAC of the password digest, kept in the session at sign-in. A new
  # password means a new digest, so sessions started before the change stop
  # matching and are dropped. nil for users without a password (OIDC).
  def credential_fingerprint
    return nil unless password_login? && password_digest.present?

    OpenSSL::HMAC.hexdigest("SHA256", self.class.credential_fingerprint_key, password_digest)
  end

  def self.credential_fingerprint_key
    Rails.application.key_generator.generate_key("rotten user credential fingerprint", 32)
  end

  private

  def password_usable
    errors.add(:password, :blank) if password_login? && password_digest.blank?
    errors.add(:password, :too_long, count: MAX_PASSWORD_BYTES) if password && password.bytesize > MAX_PASSWORD_BYTES
  end
end
