require "base64"
require "openssl"
require "securerandom"

module RottenUi
  # Worker pass keys, in exactly the format internal/auth on the server
  # checks: rotten_<api_keys.id>_<secret>, where the secret is 32 random
  # bytes as unpadded URL-safe base64 and api_keys.secret_hash holds
  # hex(sha256(secret)). spec/fixtures/pass_key_vectors.json pins this for
  # both sides.
  module PassKey
    PREFIX = "rotten_".freeze
    SECRET_BYTES = 32

    module_function

    def generate_secret
      Base64.urlsafe_encode64(SecureRandom.random_bytes(SECRET_BYTES), padding: false)
    end

    def hash_secret(secret)
      OpenSSL::Digest::SHA256.hexdigest(secret)
    end

    def token(id, secret)
      id = Integer(id.to_s, 10)
      raise ArgumentError, "pass key id must be positive" unless id.positive?

      "#{PREFIX}#{id}_#{secret}"
    end
  end
end
