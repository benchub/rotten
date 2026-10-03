# What the bin/rails users:* tasks do. Each failure raises Error with a
# message meant for the operator.
class UserAdmin
  Error = Class.new(StandardError)
  Created = Data.define(:user, :password)

  # 24 alphanumerics is about 142 bits.
  PASSWORD_LENGTH = 24

  def self.create(email, role)
    require_password_mode!
    email = normalize_email(email)
    raise Error, "give a valid email address" unless email.match?(OidcLogin::EMAIL_FORMAT)
    raise Error, "role must be viewer or admin" unless User::ROLES.include?(role)
    raise duplicate(email) if User.exists?(email: email)

    password = generate_password
    user = User.create!(email: email, role: role, provider: User::PASSWORD_PROVIDER, password: password)
    Created.new(user: user, password: password)
  rescue ActiveRecord::RecordNotUnique
    raise duplicate(email)
  end

  # Works in both modes: it's the kill switch for OIDC users too.
  def self.disable(email)
    user = find!(email)
    user.update!(active: false)
    user
  end

  # Leaves active alone, so a disabled user stays disabled.
  def self.reset_password(email)
    require_password_mode!
    user = find!(email)
    raise Error, "#{user.email} doesn't sign in with a password (provider #{user.provider.inspect})" unless user.password_login?

    password = generate_password
    user.update!(password: password)
    Created.new(user: user, password: password)
  end

  def self.require_password_mode!
    return if Rails.configuration.x.auth_mode == "password"

    raise Error, "password accounts only work with ROTTEN_UI_AUTH=password"
  end

  def self.find!(email)
    email = normalize_email(email)
    User.find_by(email: email) || raise(Error, "No user with email #{email}")
  end

  def self.normalize_email(email)
    User.normalize_value_for(:email, email.to_s)
  end

  def self.duplicate(email)
    Error.new("a user with email #{email} already exists")
  end

  def self.generate_password
    SecureRandom.alphanumeric(PASSWORD_LENGTH)
  end

  private_class_method :require_password_mode!, :find!, :normalize_email, :duplicate, :generate_password
end
