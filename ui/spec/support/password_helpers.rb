module PasswordHelpers
  def create_password_user(email:, password:, role: "viewer", active: true, **attributes)
    User.create!(email: email, password: password, role: role, active: active, provider: User::PASSWORD_PROVIDER,
                 **attributes)
  end

  def password_sign_in(email:, password:, ip: "127.0.0.1")
    post "/login", params: { email: email, password: password }, env: { "REMOTE_ADDR" => ip }
  end
end

RSpec.configure do |config|
  config.include PasswordHelpers

  # Login rate limits count in Rails.cache, a memory store in test. Each
  # example starts with fresh counters.
  config.before do
    Rails.cache.clear
  end
end
