module RottenUi
  # Extra origins for the CSP form-action directive, from
  # ROTTEN_UI_CSP_FORM_ACTION_ORIGINS: comma-separated http(s) origins with no
  # path. Chrome applies form-action to every redirect after the Sign in POST,
  # so an identity provider whose authorize endpoint isn't on the issuer's
  # origin, or that hands off to another one (federation, brokering), needs
  # those origins listed here.
  module CspFormActionOrigins
    VARIABLE = "ROTTEN_UI_CSP_FORM_ACTION_ORIGINS".freeze
    HOST = /\A[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*\z/

    def self.parse!(value)
      value.to_s.split(",").map(&:strip).reject(&:empty?).map { |entry| origin!(entry) }.uniq
    end

    def self.origin!(entry)
      uri = URI.parse(entry)
      host = uri.host.to_s.downcase
      valid = uri.is_a?(URI::HTTP) && host.match?(HOST) && uri.userinfo.nil? &&
              ["", "/"].include?(uri.path) && uri.query.nil? && uri.fragment.nil? &&
              uri.port.between?(1, 65_535) && entry.match?(%r{\Ahttps?://}i)
      raise_invalid(entry) unless valid

      port = uri.port == uri.default_port ? "" : ":#{uri.port}"
      "#{uri.scheme.downcase}://#{host}#{port}"
    rescue URI::Error
      raise_invalid(entry)
    end

    def self.raise_invalid(entry)
      raise "#{VARIABLE} must list http(s) origins with no path, such as https://login.example.com; " \
            "got #{entry.inspect}"
    end
    private_class_method :origin!, :raise_invalid
  end
end
