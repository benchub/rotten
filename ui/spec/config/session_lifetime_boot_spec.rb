require "rails_helper"
require "open3"

# Boots the app in a child process, as a deploy would, so this covers the
# initializer rather than a stubbed copy of it.
RSpec.describe "Session lifetime boot configuration" do
  def boot(lifetime)
    env = {
      "RAILS_ENV" => "production",
      "DATABASE_URL" => "postgresql://boot.invalid/none",
      "SECRET_KEY_BASE_DUMMY" => "1",
      "ROTTEN_UI_HOSTS" => "rotten.example.test",
      "ROTTEN_UI_AUTH" => "password",
      "ROTTEN_UI_SESSION_LIFETIME_HOURS" => lifetime
    }
    script = 'puts "REPORT #{Rails.configuration.x.session_lifetime_seconds}"'
    stdout, stderr, status = Open3.capture3(env, "bin/rails", "runner", script, chdir: Rails.root.to_s)
    [status, stdout.lines.grep(/\AREPORT /).last&.split&.last, stdout + stderr]
  end

  it "reads ROTTEN_UI_SESSION_LIFETIME_HOURS at boot" do
    status, seconds, output = boot("6")

    expect(status).to be_success, output
    expect(seconds).to eq("21600")
  end

  it "refuses to boot with a lifetime that isn't a positive number of hours" do
    status, seconds, output = boot("-3")

    expect(status).not_to be_success
    expect(seconds).to be_nil
    expect(output).to include('ROTTEN_UI_SESSION_LIFETIME_HOURS must be a number of hours from 0.01 to 8760, got "-3"')
  end
end
