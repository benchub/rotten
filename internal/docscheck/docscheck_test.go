package docscheck

import (
	"slices"
	"testing"
)

// uiEnvDirs are where the UI's code lives; specs, bin scripts and Dockerfiles
// aren't scanned.
var uiEnvDirs = []string{"ui/app", "ui/config", "ui/lib"}

// uiEnvExcluded are variables the UI's code reads that operators don't set,
// so the docs needn't mention them. Each must still be found by the scan,
// which keeps this list from going stale.
var uiEnvExcluded = map[string]string{
	// config/boot.rb: Rails boilerplate that points Bundler at the app's
	// Gemfile with ||=. Bundler sets it; nothing to configure.
	"BUNDLE_GEMFILE": "",
	// config/environments/test.rb eager-loads code when CI is set. Test
	// environment only.
	"CI": "",
}

// uiEnvRead are variables that Rails or Puma read themselves, not our code,
// but that operators set. The scan can't see them, so they're listed here.
var uiEnvRead = []string{
	// Rails' secret_key_base; there are no encrypted credentials in this repo.
	"SECRET_KEY_BASE",
	// Puma's worker process count.
	"WEB_CONCURRENCY",
}

func TestRubyEnvNamesFindsKnownReads(t *testing.T) {
	got := RubyEnvNames(t, uiEnvDirs...)
	for _, want := range []string{
		"DATABASE_URL",                      // ENV in database.yml
		"ROTTEN_UI_HOSTS",                   // ENV.fetch in production.rb
		"ROTTEN_UI_AUTH",                    // env["..."] in an env = ENV method
		"OIDC_ISSUER",                       // %w[...] in oidc_config.rb
		"OIDC_SCOPES",                       // value.call("...") in oidc_config.rb
		"OMNIAUTH_FAKE",                     // env["..."] in fake_login.rb
		"ROTTEN_UI_CSP_FORM_ACTION_ORIGINS", // ENV["..."] in an initializer
	} {
		if !slices.Contains(got, want) {
			t.Errorf("RubyEnvNames missed %s; got %v", want, got)
		}
	}
	if slices.Contains(got, "WEB_CONCURRENCY") {
		t.Errorf("RubyEnvNames read WEB_CONCURRENCY from a comment in puma.rb; comments should be skipped")
	}
	if slices.Contains(got, "SCRIPT_NAME") {
		t.Errorf("RubyEnvNames took SCRIPT_NAME from a Rack env hash; only process env reads count")
	}
}

func TestUIEnvDocumented(t *testing.T) {
	found := RubyEnvNames(t, uiEnvDirs...)
	for name := range uiEnvExcluded {
		if !slices.Contains(found, name) {
			t.Errorf("excluded UI env var %s is no longer read; drop it from uiEnvExcluded", name)
		}
	}
	var names []string
	for _, n := range found {
		if _, skip := uiEnvExcluded[n]; !skip {
			names = append(names, n)
		}
	}
	names = append(names, uiEnvRead...)
	RequireDocumented(t, "docs/ui.md", "UI environment variables", names)
}

// A name mentioned only in some other doc doesn't count; each set is checked
// against its own doc.
func TestMissingChecksOnlyTheGivenDoc(t *testing.T) {
	got := Missing(t, "internal/docscheck/testdata/one.md", []string{"IN_ONE", "IN_TWO", "IN_PROSE"})
	want := []string{"IN_PROSE", "IN_TWO"}
	if !slices.Equal(got, want) {
		t.Errorf("Missing = %v, want %v", got, want)
	}
	got = MissingFlags(t, "internal/docscheck/testdata/one.md", []string{"dsn", "fqdn", "listen"})
	want = []string{"-listen"}
	if !slices.Equal(got, want) {
		t.Errorf("MissingFlags = %v, want %v", got, want)
	}
}

func TestFlagNames(t *testing.T) {
	usage := "Usage of serve:\n  -config string\n    \tJSON config file\n  -dsn string\n    \tDSN (default $X)\n  -noIdleHands\n    \twatchdog\n"
	got := FlagNames(usage)
	want := []string{"config", "dsn", "noIdleHands"}
	if !slices.Equal(got, want) {
		t.Errorf("FlagNames = %v, want %v", got, want)
	}
}
