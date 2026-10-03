require "rails_helper"

# The server (internal/auth) verifies what this module makes. Both sides
# check the same vectors, in spec/fixtures/pass_key_vectors.json; the Go side
# is internal/auth/ui_contract_test.go.
RSpec.describe RottenUi::PassKey do
  vectors = JSON.parse(Rails.root.join("spec/fixtures/pass_key_vectors.json").read).fetch("vectors")

  # internal/auth.ParseKey's grammar: a decimal id with no leading zero, then
  # the secret, which is unpadded URL-safe base64 and may contain "_".
  token_grammar = /\Arotten_([1-9][0-9]*)_([A-Za-z0-9_-]+)\z/

  it "has vectors to check" do
    expect(vectors.size).to be >= 2
  end

  vectors.each do |v|
    describe "vector for key #{v['id']}" do
      it "hashes the secret as the server does" do
        expect(described_class.hash_secret(v["secret"])).to eq(v["hash"])
      end

      it "builds the token the server parses" do
        expect(described_class.token(v["id"], v["secret"])).to eq(v["token"])
      end
    end
  end

  describe ".generate_secret" do
    it "makes 32 random bytes as unpadded URL-safe base64" do
      secret = described_class.generate_secret

      expect(secret).to match(/\A[A-Za-z0-9_-]{43}\z/)
      expect(Base64.urlsafe_decode64(secret).bytesize).to eq(32)
    end

    it "makes a different secret every time" do
      expect(Array.new(50) { described_class.generate_secret }.uniq.size).to eq(50)
    end

    it "makes tokens the server's grammar splits back into id and secret" do
      secret = described_class.generate_secret
      token = described_class.token(42, secret)

      expect(token).to match(token_grammar)
      expect(token[token_grammar, 1]).to eq("42")
      expect(token[token_grammar, 2]).to eq(secret)
    end
  end

  describe ".token" do
    it "refuses an id that isn't a positive integer" do
      expect { described_class.token(0, "s") }.to raise_error(ArgumentError)
      expect { described_class.token(-1, "s") }.to raise_error(ArgumentError)
      expect { described_class.token("1x", "s") }.to raise_error(ArgumentError)
    end
  end
end
