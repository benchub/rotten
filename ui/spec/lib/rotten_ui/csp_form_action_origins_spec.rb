require "rails_helper"

RSpec.describe RottenUi::CspFormActionOrigins do
  describe ".parse!" do
    [nil, "", "  ", " , ,, "].each do |value|
      it "is empty for #{value.inspect}" do
        expect(described_class.parse!(value)).to eq([])
      end
    end

    it "reads comma-separated origins, ignoring spaces and empty entries" do
      value = " https://login.example.test, ,https://auth.example.test:8443 ,,"

      expect(described_class.parse!(value)).to eq(["https://login.example.test", "https://auth.example.test:8443"])
    end

    it "normalizes case, default ports, a bare trailing slash and duplicates" do
      value = "HTTPS://Login.Example.Test:443/, https://login.example.test, http://idp.example.test:80"

      expect(described_class.parse!(value)).to eq(["https://login.example.test", "http://idp.example.test"])
    end

    [
      "login.example.test",
      "//login.example.test",
      "ftp://login.example.test",
      "javascript:alert(1)",
      "data:text/html",
      "https://login.example.test/oauth2/authorize",
      "https://login.example.test?x=1",
      "https://login.example.test#x",
      "https://user:pass@login.example.test",
      "https://*.example.test",
      "*",
      "https:",
      "'self'",
      "'unsafe-inline'",
      "https://login.example.test; script-src *",
      "https://login.example.test https://other.example.test",
      "https://bad_host.example.test",
      "https://login.example.test:99999"
    ].each do |entry|
      it "refuses #{entry.inspect}, naming the variable and the entry" do
        expect { described_class.parse!("https://ok.example.test, #{entry}") }
          .to raise_error(RuntimeError, /ROTTEN_UI_CSP_FORM_ACTION_ORIGINS.*#{Regexp.escape(entry.inspect)}/)
      end
    end
  end
end
