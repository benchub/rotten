require "rails_helper"

# The CLI (internal/auth.NormalizeFQDN) checks the same vectors, in
# spec/fixtures/fqdn_vectors.json; the Go side is internal/auth/fqdn_test.go.
RSpec.describe ApiKeyIssue, "FQDN" do
  vectors = JSON.parse(Rails.root.join("spec/fixtures/fqdn_vectors.json").read)

  it "has vectors to check" do
    expect(vectors.fetch("valid").size).to be >= 5
    expect(vectors.fetch("invalid").size).to be >= 5
  end

  vectors.fetch("valid").each do |v|
    it "accepts #{v['input'].inspect} as #{v['fqdn'].inspect}" do
      issue = described_class.new(name: "k", fqdn: v["input"])
      issue.validate

      expect(issue.errors[:fqdn]).to be_empty
      expect(issue.fqdn).to eq(v["fqdn"])
    end
  end

  vectors.fetch("invalid").each do |input|
    it "rejects #{input.inspect}" do
      issue = described_class.new(name: "k", fqdn: input)
      issue.validate

      expect(issue.errors[:fqdn]).not_to be_empty
    end
  end
end
