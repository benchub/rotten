require "rails_helper"
require "erb"

RSpec.describe "database configuration" do
  it "requires DATABASE_URL in production" do
    original_database_url = ENV.delete("DATABASE_URL")
    original_rails_env = ENV["RAILS_ENV"]
    ENV["RAILS_ENV"] = "production"

    expect do
      ERB.new(Rails.root.join("config/database.yml").read).result
    end.to raise_error(KeyError, /DATABASE_URL/)
  ensure
    ENV["DATABASE_URL"] = original_database_url if original_database_url
    ENV["RAILS_ENV"] = original_rails_env
  end
end
