require "spec_helper"

ENV["RAILS_ENV"] ||= "test"
ENV["ROTTEN_UI_AUTH"] ||= "password"
require_relative "../config/environment"
abort("The Rails environment is running in production mode!") if Rails.env.production?

require "rspec/rails"
require "capybara/rspec"
require "factory_bot_rails"
require "shoulda/matchers"

Rails.root.glob("spec/support/**/*.rb").sort.each { |file| require file }

Shoulda::Matchers.configure do |config|
  config.integrate do |with|
    with.test_framework :rspec
    with.library :rails
  end
end

RSpec.configure do |config|
  config.include FactoryBot::Syntax::Methods
  config.fixture_paths = []
  config.use_transactional_fixtures = false
  config.infer_spec_type_from_file_location!
  config.filter_rails_from_backtrace!

  config.before(:each, type: :system) do
    driven_by :selenium, using: :headless_chrome, screen_size: [1400, 1400] do |options|
      options.add_argument("--disable-dev-shm-usage")
      options.add_argument("--no-sandbox")
      # Lets spec/security/csp_browser_spec.rb read CSP violations.
      options.add_option("goog:loggingPrefs", { browser: "ALL" })
      options.binary = ENV["CHROME_BIN"] if ENV["CHROME_BIN"].present?
    end
  end
end
