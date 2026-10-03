require "rails_helper"
require "brakeman"
require "bundler/audit/database"
require "bundler/audit/scanner"

# Brakeman and bundler-audit, as specs, so `make test-ui` is the gate for
# them too. Brakeman runs once for the file. bundler-audit reads the advisory
# database fetched when the dev image is built (ui/dev.Dockerfile), so this
# runs offline; rebuild the image with --no-cache to pick up new advisories.
RSpec.describe "Static security analysis" do
  describe "Brakeman" do
    before(:context) do
      @tracker = Brakeman.run(app_path: Rails.root.to_s, quiet: true, print_report: false, report_progress: false)
    end

    it "finds no warnings" do
      warnings = @tracker.filtered_warnings.map { |w| "#{w.warning_type} #{w.file.relative}:#{w.line} #{w.message}" }
      expect(warnings).to be_empty
    end

    it "parses every file" do
      expect(@tracker.errors).to be_empty
    end

    it "has no ignore or config file, so nothing is waved through" do
      expect(Rails.root.join("config/brakeman.ignore")).not_to exist
      expect(Rails.root.join("config/brakeman.yml")).not_to exist
    end
  end

  describe "bundler-audit" do
    let(:database) do
      expect(Bundler::Audit::Database).to exist,
        "no advisory database at #{Bundler::Audit::Database.path}; rebuild the dev image (make ui-image)"
      Bundler::Audit::Database.new
    end

    def advisories(root)
      Bundler::Audit::Scanner.new(root.to_s, "Gemfile.lock", database).scan.map do |result|
        case result
        when Bundler::Audit::Results::InsecureSource then "insecure source #{result.source}"
        else "#{result.gem.name} #{result.gem.version}: #{result.advisory.id} #{result.advisory.title}"
        end
      end
    end

    it "has an advisory database to check against" do
      expect(database.size).to be > 100
    end

    it "has no config file, which could ignore advisories" do
      expect(Rails.root.join(".bundler-audit.yml")).not_to exist
    end

    it "finds no gems with known vulnerabilities" do
      expect(advisories(Rails.root)).to be_empty
    end

    it "flags a lockfile with a known-vulnerable gem, so the check above can fail" do
      root = Rails.root.join("tmp/bundler-audit-fixture")
      FileUtils.mkdir_p(root)
      # rack 2.0.1 has many published advisories, such as CVE-2018-16471.
      root.join("Gemfile.lock").write(<<~LOCK)
        GEM
          remote: https://rubygems.org/
          specs:
            rack (2.0.1)

        PLATFORMS
          ruby

        DEPENDENCIES
          rack (= 2.0.1)

        BUNDLED WITH
           2.5.0
      LOCK

      expect(advisories(root)).to include(a_string_starting_with("rack 2.0.1: "))
    ensure
      FileUtils.rm_rf(root)
    end
  end
end
