module RottenUi
  # How long a session lasts after login, from ROTTEN_UI_SESSION_LIFETIME_HOURS.
  # Unset or blank means 12 hours. The expiry is absolute: activity doesn't
  # extend it.
  class SessionLifetime
    DEFAULT_HOURS = 12
    MIN_HOURS = 0.01
    MAX_HOURS = 8760
    MESSAGE = "ROTTEN_UI_SESSION_LIFETIME_HOURS must be a number of hours from #{MIN_HOURS} to #{MAX_HOURS}".freeze

    # Whole seconds.
    def self.fetch!(env = ENV)
      value = env["ROTTEN_UI_SESSION_LIFETIME_HOURS"].to_s.strip
      return DEFAULT_HOURS * 3600 if value.empty?

      hours = Float(value, exception: false)
      raise "#{MESSAGE}, got #{value.inspect}" unless hours&.finite? && hours >= MIN_HOURS && hours <= MAX_HOURS

      (hours * 3600).round
    end
  end
end
