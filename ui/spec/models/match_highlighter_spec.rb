require "rails_helper"

# Whether Postgres folds ASCII case the way Ruby does. In a Turkish or
# Azeri locale it doesn't: lower('I') is 'ı' and upper('i') is 'İ', so with a
# tr_TR.UTF-8 libc database, 'I users' ~* 'i|users' matches only "users".
RSpec.describe MatchHighlighter do
  describe ".ascii_folding_safe_for?" do
    # pg_database rows as to_jsonb gives them: PG 14 has no datlocprovider;
    # 15 and 16 have daticulocale; 17 and up have datlocale.
    let(:en_us) { { "datcollate" => "en_US.utf8", "datctype" => "en_US.utf8" } }

    it "is safe for the usual locales, on every catalog shape" do
      expect(described_class.ascii_folding_safe_for?(en_us, [])).to be(true)
      expect(described_class.ascii_folding_safe_for?(en_us.merge("datlocprovider" => "c", "daticulocale" => nil), [])).to be(true)
      expect(described_class.ascii_folding_safe_for?({ "datcollate" => "C", "datctype" => "C", "datlocprovider" => "b",
                                                       "datlocale" => "C.UTF-8" }, [])).to be(true)
      expect(described_class.ascii_folding_safe_for?(en_us.merge("datlocprovider" => "i", "datlocale" => "en-US"), [])).to be(true)
      expect(described_class.ascii_folding_safe_for?(en_us.merge("datctype" => "trk"), [])).to be(true)
    end

    it "is unsafe for a Turkish or Azeri libc database, on PG 14 and later" do
      expect(described_class.ascii_folding_safe_for?({ "datcollate" => "tr_TR.UTF-8", "datctype" => "tr_TR.UTF-8" }, [])).to be(false)
      expect(described_class.ascii_folding_safe_for?(en_us.merge("datctype" => "tr_TR.UTF-8", "datlocprovider" => "c"), [])).to be(false)
      expect(described_class.ascii_folding_safe_for?(en_us.merge("datcollate" => "az_AZ.UTF-8"), [])).to be(false)
      expect(described_class.ascii_folding_safe_for?(en_us.merge("datctype" => "TR_tr"), [])).to be(false)
      expect(described_class.ascii_folding_safe_for?(en_us.merge("datctype" => "Turkish_Turkey.1254"), [])).to be(false)
    end

    it "is unsafe for a Turkish or Azeri ICU database" do
      expect(described_class.ascii_folding_safe_for?(en_us.merge("datlocprovider" => "i", "daticulocale" => "tr-TR"), [])).to be(false)
      expect(described_class.ascii_folding_safe_for?(en_us.merge("datlocprovider" => "i", "datlocale" => "az-Latn-AZ"), [])).to be(false)
      expect(described_class.ascii_folding_safe_for?(en_us.merge("datlocprovider" => "i", "datlocale" => "tr"), [])).to be(false)
    end

    it "is unsafe when a filtered column has a Turkish or Azeri collation" do
      expect(described_class.ascii_folding_safe_for?(en_us, [{ "collprovider" => "c", "collcollate" => "C", "collctype" => "C" }])).to be(true)
      expect(described_class.ascii_folding_safe_for?(en_us, [{ "collprovider" => "c", "collcollate" => "tr_TR.utf8",
                                                               "collctype" => "tr_TR.utf8" }])).to be(false)
      expect(described_class.ascii_folding_safe_for?(en_us, [{ "collprovider" => "i", "colliculocale" => "tr-TR" }])).to be(false)
      expect(described_class.ascii_folding_safe_for?(en_us, [{ "collprovider" => "i", "colllocale" => "az" }])).to be(false)
    end
  end

  # Real Postgres. The test image has no tr_TR libc locale, so this gives a
  # filtered column the ICU tr-TR collation it does have, and checks the
  # catalog lookup sees it. (ICU's regex case folding isn't Turkish, but the
  # lookup treats any tr or az locale as unsafe, so it stands in for libc.)
  describe ".lookup_ascii_folding_safe" do
    let(:lookup) { ApplicationRecord.with_connection { |conn| described_class.lookup_ascii_folding_safe(conn) } }

    it "is safe on the test database" do
      expect(lookup).to be(true)
    end

    it "is unsafe once a filtered column has a Turkish collation" do
      owner = ReportFixture.connect
      owner.exec('alter table rotten.controllers alter column controller type text collate "tr-TR-x-icu"')
      expect(lookup).to be(false)
    ensure
      owner&.exec('alter table rotten.controllers alter column controller type text collate "default"')
      owner&.close
    end
  end

  describe ".ascii_folding_safe?" do
    after { described_class.forget_ascii_folding }

    it "looks it up once per process" do
      described_class.forget_ascii_folding
      allow(described_class).to receive(:lookup_ascii_folding_safe).and_return(false)

      expect(described_class.ascii_folding_safe?).to be(false)
      expect(described_class.ascii_folding_safe?).to be(false)
      expect(described_class).to have_received(:lookup_ascii_folding_safe).once
    end

    it "is unsafe, without remembering it, when the lookup fails" do
      described_class.forget_ascii_folding
      allow(described_class).to receive(:lookup_ascii_folding_safe).and_raise(ActiveRecord::ConnectionNotEstablished)

      expect(described_class.ascii_folding_safe?).to be(false)
      expect(described_class.ascii_folding_safe?).to be(false)
      expect(described_class).to have_received(:lookup_ascii_folding_safe).twice
    end
  end

  describe "#spans where ASCII folding isn't safe" do
    def spans(pattern, text, safe:) = described_class.new(pattern, ascii_folding_safe: safe).spans(text)

    it "marks nothing for a pattern with an i, or with a bracket that could hold one" do
      expect(spans("i|users", "I users", safe: false)).to be_empty
      expect(spans("I|users", "i users", safe: false)).to be_empty
      expect(spans("[h-j]|users", "I users", safe: false)).to be_empty
      expect(spans("[[:lower:]]|users", "I users", safe: false)).to be_empty
      expect(spans("i|users", "I users", safe: true)).to eq([[0, 1], [2, 7]])
    end

    it "still marks patterns without them" do
      expect(spans("USERS|\\d", "I users 1", safe: false)).to eq([[2, 7], [8, 9]])
    end
  end
end
