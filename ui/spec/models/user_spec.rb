require "rails_helper"

RSpec.describe User, type: :model do
  before do
    User.delete_all
  end

  it "stores email stripped and lowercased" do
    user = User.create!(email: "  Alice@X.com ", role: "viewer")

    expect(user.reload.email).to eq("alice@x.com")
  end

  it "finds a user by email regardless of case" do
    user = User.create!(email: "Alice@X.com", role: "viewer")

    expect(User.find_by(email: "alice@x.com")).to eq(user)
    expect(User.find_by(email: "ALICE@x.COM")).to eq(user)
  end

  it "rejects a duplicate email that differs only in case at the database" do
    User.create!(email: "alice@x.com", role: "viewer")

    expect do
      User.connection.execute("insert into users (email, role) values ('Alice@X.com', 'viewer')")
    end.to raise_error(ActiveRecord::RecordNotUnique)
  end
end
