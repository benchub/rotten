require "rails_helper"

RSpec.describe "Password login security", type: :request do
  let(:password) { "security-spec-password-123" }

  before do
    User.delete_all
  end

  describe "CSRF" do
    around do |example|
      original = ActionController::Base.allow_forgery_protection
      ActionController::Base.allow_forgery_protection = true
      example.run
    ensure
      ActionController::Base.allow_forgery_protection = original
    end

    it "refuses a login POST without an authenticity token" do
      create_password_user(email: "viewer@example.test", password: password)

      password_sign_in(email: "viewer@example.test", password: password)

      expect(response).to have_http_status(:unprocessable_content)
      expect(session[:user_id]).to be_nil
    end

    it "accepts a login POST carrying the form's authenticity token" do
      user = create_password_user(email: "viewer@example.test", password: password)
      get "/login"
      token = Nokogiri::HTML(response.body).at_css("form[action='/login'] input[name='authenticity_token']")["value"]

      post "/login", params: { email: "viewer@example.test", password: password, authenticity_token: token }

      expect(response).to redirect_to("/")
      expect(session[:user_id]).to eq(user.id)
    end
  end

  describe "secrets in logs" do
    it "filters passwords and digests from logged parameters" do
      filter = ActiveSupport::ParameterFilter.new(Rails.application.config.filter_parameters)

      filtered = filter.filter("password" => "hunter2", "password_digest" => "$2a$digest", "email" => "a@b.test")

      expect(filtered).to eq("password" => "[FILTERED]", "password_digest" => "[FILTERED]", "email" => "[FILTERED]")
    end

    it "keeps the digest out of User#inspect" do
      user = create_password_user(email: "viewer@example.test", password: password)

      expect(user.inspect).not_to include(user.password_digest)
      expect(user.inspect).not_to include(password)
    end

    it "doesn't write the password to the request log" do
      create_password_user(email: "viewer@example.test", password: password)
      log = StringIO.new
      logger = ActiveSupport::Logger.new(log)
      original = ActionController::Base.logger
      ActionController::Base.logger = logger
      begin
        password_sign_in(email: "viewer@example.test", password: password)
        password_sign_in(email: "viewer@example.test", password: "wrong-#{password}")
      ensure
        ActionController::Base.logger = original
      end

      expect(log.string).to include("SessionsController#create")
      expect(log.string).not_to include(password)
    end
  end
end
