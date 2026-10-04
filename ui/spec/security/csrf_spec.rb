require "rails_helper"
require "open3"

# Every route that isn't GET or HEAD must refuse a request that doesn't carry
# the session's own authenticity token. The routes come from the app's route
# set, so a new route is covered as soon as it's drawn.
#
# Development-only routes, like POST /auth/fake/:persona (OMNIAUTH_FAKE=1),
# aren't in the test route set. The spec redraws config/routes.rb with the
# fake login on and checks those routes too, with Rails.env.development?
# stubbed so the controller's own gate lets a forged request through if the
# token check doesn't stop it. A child process booted in development checks
# that the redraw matches what development really draws. That production and
# test boots with OMNIAUTH_FAKE=1 don't draw the fake route is covered in
# spec/config/oidc_boot_spec.rb.
#
# Not in the route set, and covered elsewhere:
# - POST /auth/openid_connect is OmniAuth middleware (spec/security/oidc_csrf_spec.rb).
# - GET /auth/openid_connect/callback changes state on a GET; OmniAuth's state
#   parameter and PKCE protect it, not an authenticity token.
# - GET /__test/sign_in signs in on a GET, but it's only drawn in the test env.
module CsrfSpecRoutes
  SAFE_VERBS = %w[GET HEAD].freeze

  # Values for path segments. A route with a segment missing here fails the
  # spec until one is added.
  PATH_SAMPLES = { persona: "viewer", id: "1" }.freeze

  # Routes exempt from the check, as "VERB /path" => "reason". Keep this empty
  # unless a route really can't carry a token.
  EXEMPT = {}.freeze

  Route = Data.define(:verb, :path, :spec, :dev_only) do
    def label = "#{verb} #{spec}"
  end

  # Redraws config/routes.rb the way a development boot with OMNIAUTH_FAKE=1
  # draws it, then restores the test route set.
  def self.with_dev_only_routes
    fake_login = Rails.configuration.x.fake_login
    Rails.configuration.x.fake_login = true
    Rails.application.reload_routes!
    yield
  ensure
    Rails.configuration.x.fake_login = fake_login
    Rails.application.reload_routes!
  end

  def self.all
    test_routes = state_changing(dev_only: false)
    dev_routes = with_dev_only_routes { state_changing(dev_only: true) }
    test_labels = test_routes.map(&:label)
    test_routes + dev_routes.reject { |route| test_labels.include?(route.label) }
  end

  # Non-GET routes in a booted app, as "VERB /path" labels.
  def self.label_script
    <<~RUBY
      labels = Rails.application.routes.routes.flat_map do |route|
        spec = route.path.spec.to_s.delete_suffix("(.:format)")
        verbs = route.verb.to_s.split("|")
        verbs = %w[POST] if verbs.empty?
        (verbs - #{SAFE_VERBS.inspect}).map { |verb| "\#{verb} \#{spec}" }
      end
      puts "ROUTES " + labels.uniq.to_json
    RUBY
  end

  def self.development_boot_labels
    env = {
      "RAILS_ENV" => "development",
      "ROTTEN_UI_AUTH" => "oidc",
      "OMNIAUTH_FAKE" => "1",
      "DATABASE_URL" => "postgresql://boot.invalid/none"
    }
    stdout, stderr, status = Open3.capture3(env, "bin/rails", "runner", label_script, chdir: Rails.root.to_s)
    line = stdout.lines.grep(/\AROUTES /).last
    raise "development boot failed:\n#{stdout}#{stderr}" unless status.success? && line

    JSON.parse(line.delete_prefix("ROUTES "))
  end

  def self.state_changing(dev_only:)
    Rails.application.routes.routes.flat_map do |route|
      spec = route.path.spec.to_s.delete_suffix("(.:format)")
      verbs = route.verb.to_s.split("|")
      verbs = %w[POST] if verbs.empty?

      (verbs - SAFE_VERBS).filter_map do |verb|
        next if EXEMPT.key?("#{verb} #{spec}")

        params = route.required_parts.to_h do |part|
          sample = PATH_SAMPLES.fetch(part) { raise "Add a PATH_SAMPLES value for :#{part} in #{spec}" }
          requirement = route.requirements[part]
          raise "PATH_SAMPLES[:#{part}] doesn't match #{spec}" if requirement.is_a?(Regexp) && !requirement.match?(sample)

          [part, sample]
        end
        Route.new(verb: verb, path: route.format(params), spec: spec, dev_only: dev_only)
      end
    end.uniq(&:label)
  end
end

RSpec.describe "CSRF protection on every state-changing route", type: :request do
  routes = CsrfSpecRoutes.all

  let(:password) { "csrf-spec-password-123" }

  before { User.delete_all }

  let!(:victim) { create_password_user(email: "victim@example.test", password: password) }
  let!(:attacker) { create_password_user(email: "attacker@example.test", password: password, role: "admin") }

  # What a forged request would ask for: sign in as the attacker, sign out the
  # victim, and so on. Routes without an entry get no params.
  def forged_params(route)
    case route.label
    when "POST /login" then { email: attacker.email, password: password }
    when "POST /__test/sign_in" then { user_id: attacker.id }
    when "PATCH /password"
      { current_password: password, password: "forged-new-password-1", password_confirmation: "forged-new-password-1" }
    else {}
    end
  end

  def csrf_token_from(body)
    Nokogiri::HTML(body).at_css("meta[name='csrf-token']")&.[]("content").presence ||
      raise("no csrf-token meta tag in the page")
  end

  # The victim signs in through the real form, with forgery protection on.
  def sign_in_victim
    get "/login"
    post "/login", params: { email: victim.email, password: password, authenticity_token: csrf_token_from(response.body) }
    expect(response).to redirect_to("/")
    get "/"
    expect(response.body).to include(victim.email)
  end

  def forge(route, token: :none, headers: {})
    params = forged_params(route)
    params = params.merge(authenticity_token: token) unless token == :none
    process(route.verb.downcase.to_sym, route.path, params: params, headers: headers)
  end

  def expect_rejected_and_victim_still_signed_in
    expect(response).to have_http_status(:unprocessable_content)
    get "/"
    expect(response).to have_http_status(:ok)
    expect(response.body).to include(victim.email)
  end

  around do |example|
    original = ActionController::Base.allow_forgery_protection
    ActionController::Base.allow_forgery_protection = true
    example.run
  ensure
    ActionController::Base.allow_forgery_protection = original
  end

  it "finds the app's state-changing routes" do
    expect(routes.map(&:label)).to include("POST /login", "DELETE /logout", "POST /__test/sign_in",
                                           "POST /admin/keys", "POST /admin/keys/:id/revoke", "PATCH /password")
  end

  it "finds the development-only routes" do
    expect(routes.select(&:dev_only).map(&:label)).to eq(["POST /auth/fake/:persona"])
  end

  it "covers every state-changing route a development boot with OMNIAUTH_FAKE=1 draws" do
    labels = CsrfSpecRoutes.development_boot_labels

    expect(labels).to include("POST /auth/fake/:persona")
    expect(labels - routes.map(&:label)).to be_empty
  end

  routes.each do |route|
    describe route.label do
      if route.dev_only
        around do |example|
          CsrfSpecRoutes.with_dev_only_routes { example.run }
        end
      end

      before { sign_in_victim }

      # The victim's session was started in password mode; switching now
      # leaves it signed in while the fake login can sign in a persona.
      if route.dev_only
        before do
          allow(Rails.env).to receive(:development?).and_return(true)
          use_oidc_mode(viewer_group: "dev-viewers", admin_group: "dev-admins")
        end
      end

      it "refuses a request with no authenticity token" do
        forge(route)

        expect_rejected_and_victim_still_signed_in
      end

      it "refuses a made-up authenticity token" do
        forge(route, token: Base64.strict_encode64(SecureRandom.random_bytes(32)))

        expect_rejected_and_victim_still_signed_in
      end

      it "refuses another session's authenticity token" do
        other = open_session
        other.get "/login"
        foreign_token = csrf_token_from(other.response.body)

        forge(route, token: foreign_token)

        expect_rejected_and_victim_still_signed_in
      end

      it "refuses the session's own token from a foreign Origin" do
        forge(route, token: csrf_token_from(response.body), headers: { "Origin" => "https://evil.example" })

        expect_rejected_and_victim_still_signed_in
      end

      it "accepts the session's own token" do
        forge(route, token: csrf_token_from(response.body))

        expect(response).not_to have_http_status(:unprocessable_content)
        # Proves the controller's gate was open, so the refusals above came
        # from the token check.
        expect(response).to redirect_to("/") if route.dev_only
      end
    end
  end
end
