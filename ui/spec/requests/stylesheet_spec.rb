require "rails_helper"
require "open3"
require "tmpdir"

# The layout serves one stylesheet: Tailwind's build of
# app/assets/tailwind/application.css. `make test-ui` and the dev stack build
# it first; without that build the page has no design at all.
RSpec.describe "Stylesheet", type: :request do
  let(:built) { Rails.root.join("app/assets/builds/tailwind.css") }

  def stylesheet_hrefs
    Nokogiri::HTML5(response.body).css("link[rel=stylesheet]").map { |link| link["href"] }
  end

  it "links only the Tailwind build, and the build resolves" do
    get "/login"

    hrefs = stylesheet_hrefs
    expect(hrefs.size).to eq(1)
    expect(hrefs.first).to match(%r{\A/assets/tailwind-\h+\.css\z})

    get hrefs.first
    expect(response).to have_http_status(:ok)
    expect(response.media_type).to eq("text/css")
    expect(response.body).to include("tailwindcss v4")
  end

  it "builds the design: theme tokens, components and the utilities the views use" do
    css = built.read

    expect(css).to include("--color-primary:")
    expect(css).to match(/\.card\s*\{/)
    expect(css).to match(/\.btn-primary\s*\{/)
    expect(css).to match(/\.sr-only\s*\{/)
  end

  # Tailwind generates utilities from every file it scans (views, helpers,
  # JavaScript, anything its source detection finds under Rails.root), not just
  # app/assets/tailwind. Rather than guess that set from mtimes, rebuild with
  # the gem's own command into a temp file and require the same bytes, so a
  # class added anywhere without a rebuild fails here.
  it "matches a fresh build of the current sources" do
    expect(built).to exist

    Dir.mktmpdir do |dir|
      fresh = File.join(dir, "tailwind.css")
      command = Tailwindcss::Commands.compile_command(silent: true)
      command[command.index("-o") + 1] = fresh

      _out, err, status = Open3.capture3(*command, chdir: Rails.root.to_s)
      expect(status).to be_success, err

      # A boolean, so a failure prints the hint, not a diff of minified CSS.
      expect(File.binread(fresh) == built.binread).to be(true),
        "app/assets/builds/tailwind.css is stale; run bin/rails tailwindcss:build"
    end
  end

  it "serves the built artifact unchanged" do
    get "/login"
    get stylesheet_hrefs.first

    expect(response.body.b == built.binread).to be(true),
      "the served stylesheet differs from app/assets/builds/tailwind.css"
  end
end
