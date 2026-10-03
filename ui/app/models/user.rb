class User < ApplicationRecord
  ROLES = %w[viewer admin].freeze

  normalizes :email, with: ->(email) { email.strip.downcase }

  validates :email, presence: true
  validates :role, inclusion: { in: ROLES }

  def admin?
    role == "admin"
  end
end
