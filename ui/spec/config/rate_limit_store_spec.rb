require "rails_helper"
require "open3"

# The login rate limits count in the controller's cache store. A null store
# silently turns them off, so check what each environment really boots with.
RSpec.describe "Login rate-limit store" do
  %w[production development].each do |rails_env|
    it "counts in a memory store in #{rails_env}" do
      env = {
        "RAILS_ENV" => rails_env,
        "DATABASE_URL" => "postgresql://boot.invalid/none",
        "SECRET_KEY_BASE_DUMMY" => "1",
        "ROTTEN_UI_HOSTS" => "rotten.example.test",
        "ROTTEN_UI_AUTH" => "password"
      }
      script = 'puts "STORE #{SessionsController.cache_store.class.name}"'

      stdout, stderr, status = Open3.capture3(env, "bin/rails", "runner", script, chdir: Rails.root.to_s)

      expect(status).to be_success, stdout + stderr
      expect(stdout).to include("STORE ActiveSupport::Cache::MemoryStore")
    end
  end

  it "counts in a memory store in test" do
    expect(SessionsController.cache_store).to be_a(ActiveSupport::Cache::MemoryStore)
  end
end
