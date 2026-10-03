require "rails_helper"
require "open3"

# Boots the app in production in a child process and sends requests through
# the full middleware stack, for the settings the test env doesn't have:
# force_ssl's Secure cookies and HSTS, and Host authorization.
module ProductionBootSpec
  HOST = "rotten.example.test".freeze

  def self.boot(env, script)
    base = {
      "RAILS_ENV" => "production",
      "DATABASE_URL" => "postgresql://boot.invalid/none",
      "SECRET_KEY_BASE_DUMMY" => "1",
      "ROTTEN_UI_AUTH" => "password",
      "ROTTEN_UI_HOSTS" => nil
    }
    stdout, stderr, status = Open3.capture3(base.merge(env), "bin/rails", "runner", script, chdir: Rails.root.to_s)
    report = stdout.lines.grep(/\AREPORT /).last
    [status, report && JSON.parse(report.delete_prefix("REPORT ")), stdout + stderr]
  end

  # Each probe is [name, path, headers]. The report maps name to status and
  # lowercased headers.
  def self.probe_script(probes)
    <<~RUBY
      probes = JSON.parse(#{probes.to_json.inspect})
      results = probes.to_h do |name, path, headers|
        env = Rack::MockRequest.env_for("https://#{HOST}" + path, "REMOTE_ADDR" => "192.0.2.10")
        headers.each { |key, value| env["HTTP_" + key.upcase.tr("-", "_")] = value }
        status, response_headers, body = Rails.application.call(env)
        body.close if body.respond_to?(:close)
        [name, { "status" => status, "headers" => response_headers.to_h.transform_keys(&:downcase) }]
      end
      puts "REPORT " + results.to_json
    RUBY
  end

  PROBES = [
    ["login", "/login", { "Host" => HOST }],
    ["login_with_port", "/login", { "Host" => "#{HOST}:443" }],
    ["other_listed_host", "/login", { "Host" => "rotten-alias.example.test" }],
    ["bad_host", "/login", { "Host" => "evil.example" }],
    ["bad_forwarded_host", "/login", { "Host" => HOST, "X-Forwarded-Host" => "evil.example" }],
    ["subdomain_of_listed_host", "/login", { "Host" => "evil.#{HOST}" }],
    ["health_by_ip", "/up", { "Host" => "10.1.2.3" }]
  ].freeze
end

RSpec.describe "Production security settings" do
  before(:context) do
    hosts = " #{ProductionBootSpec::HOST}, rotten-alias.example.test ,,"
    env = { "ROTTEN_UI_HOSTS" => hosts,
            "ROTTEN_UI_CSP_FORM_ACTION_ORIGINS" => " https://login.example.test, https://Broker.example.test:8443/ ," }
    @status, @report, @output = ProductionBootSpec.boot(env, ProductionBootSpec.probe_script(ProductionBootSpec::PROBES))
  end

  def probe(name)
    expect(@status).to be_success, @output
    @report.fetch(name)
  end

  def session_cookie_attributes(result)
    cookies = Array(result["headers"]["set-cookie"]).flat_map { |header| header.split("\n") }
    cookie = cookies.find { |line| line.start_with?("#{Rails.application.config.session_options.fetch(:key)}=") }
    expect(cookie).to be_present, "no session cookie in #{cookies.inspect}"
    cookie.split(";").map { |part| part.strip.downcase }.drop(1)
  end

  describe "session cookie" do
    it "is Secure, HttpOnly and SameSite=Lax" do
      expect(session_cookie_attributes(probe("login"))).to include("secure", "httponly", "samesite=lax")
    end
  end

  describe "headers" do
    it "sends HSTS for two years, including subdomains" do
      expect(probe("login")["headers"]["strict-transport-security"]).to eq("max-age=63072000; includeSubDomains")
    end

    it "sends the CSP and the other security headers" do
      headers = probe("login")["headers"]

      expect(headers["content-security-policy"]).to include("default-src 'self'", "frame-ancestors 'none'")
      expect(headers).to include("x-frame-options" => "DENY", "x-content-type-options" => "nosniff")
      expect(headers["permissions-policy"]).to include("camera=()")
    end

    it "adds the origins in ROTTEN_UI_CSP_FORM_ACTION_ORIGINS to form-action" do
      form_action = probe("login")["headers"]["content-security-policy"].split(";").map(&:strip).grep(/\Aform-action /)

      expect(form_action).to eq(["form-action 'self' https://login.example.test https://broker.example.test:8443"])
    end

    it "refuses to boot when ROTTEN_UI_CSP_FORM_ACTION_ORIGINS has an entry that isn't an origin" do
      env = { "ROTTEN_UI_HOSTS" => ProductionBootSpec::HOST,
              "ROTTEN_UI_CSP_FORM_ACTION_ORIGINS" => "https://login.example.test, https://evil.example/path" }
      status, report, output = ProductionBootSpec.boot(env, 'puts "REPORT {}"')

      expect(status).not_to be_success
      expect(report).to be_nil
      expect(output).to include("ROTTEN_UI_CSP_FORM_ACTION_ORIGINS", "https://evil.example/path")
    end
  end

  describe "Host header" do
    it "serves the hosts in ROTTEN_UI_HOSTS, with or without a port" do
      expect(probe("login")["status"]).to eq(200)
      expect(probe("login_with_port")["status"]).to eq(200)
      expect(probe("other_listed_host")["status"]).to eq(200)
    end

    it "refuses any other Host with 403" do
      expect(probe("bad_host")["status"]).to eq(403)
      expect(probe("subdomain_of_listed_host")["status"]).to eq(403)
    end

    it "refuses an unlisted X-Forwarded-Host with 403" do
      expect(probe("bad_forwarded_host")["status"]).to eq(403)
    end

    it "lets health checks reach /up by IP address" do
      # The database is unreachable here, so /up answers 503, not 403.
      expect(probe("health_by_ip")["status"]).to eq(503)
    end

    ["unset", "blank"].each do |state|
      it "refuses to boot when ROTTEN_UI_HOSTS is #{state}" do
        value = state == "unset" ? nil : " , "
        status, report, output = ProductionBootSpec.boot({ "ROTTEN_UI_HOSTS" => value }, 'puts "REPORT {}"')

        expect(status).not_to be_success
        expect(report).to be_nil
        expect(output).to include("RAILS_ENV=production requires ROTTEN_UI_HOSTS")
      end
    end
  end
end
