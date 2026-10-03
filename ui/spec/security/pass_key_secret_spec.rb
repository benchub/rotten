require "rails_helper"

# A new pass key's secret exists only in the create response. It must not
# reach the log, the session cookie, the flash, or a cache.
RSpec.describe "Pass key secrets", :api_keys, type: :request do
  before do
    User.delete_all
    admin = User.create!(email: "secret.admin@example.test", role: "admin", active: true)
    post "/__test/sign_in", params: { user_id: admin.id }
  end

  def create_and_capture_log
    log = StringIO.new
    logger = ActiveSupport::Logger.new(log)
    original = [Rails.logger, ActiveRecord::Base.logger, ActionController::Base.logger, ActionView::Base.logger]
    Rails.logger = ActiveRecord::Base.logger = ActionController::Base.logger = ActionView::Base.logger = logger
    logger.level = :debug
    post "/admin/keys", params: { api_key: { name: "secret-check", fqdn: "db.example.test" } }
    log.string
  ensure
    Rails.logger, ActiveRecord::Base.logger, ActionController::Base.logger, ActionView::Base.logger = original
  end

  it "keeps the secret out of the log, even at debug level" do
    log = create_and_capture_log
    token = Nokogiri::HTML(response.body).at_css("#pass-key-token").text.strip
    secret = token.split("_", 3).last

    expect(log).to include("ApiKeysController#create")
    expect(log).not_to include(secret)
  end

  it "sends the secret with no-store, so no browser or proxy cache keeps it" do
    post "/admin/keys", params: { api_key: { name: "secret-check", fqdn: "db.example.test" } }

    expect(response).to have_http_status(:created)
    expect(response.headers["cache-control"]).to eq("no-store")
  end

  it "doesn't put the secret in the session cookie" do
    post "/admin/keys", params: { api_key: { name: "secret-check", fqdn: "db.example.test" } }
    secret = Nokogiri::HTML(response.body).at_css("#pass-key-token").text.strip.split("_", 3).last

    # The cookie store serializes exactly this hash, flash included, into the
    # encrypted cookie, so checking it covers what the browser keeps.
    stored = session.to_hash
    expect(stored).to include("user_id")
    expect(stored.to_s).not_to include(secret)
    expect(flash.to_hash.to_s).not_to include(secret)
    expect(cookies.to_hash.to_s).not_to include(secret)
  end
end
