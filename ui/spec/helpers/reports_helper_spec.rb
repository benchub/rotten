require "rails_helper"

RSpec.describe ReportsHelper, type: :helper do
  def cell(column_key, type, value)
    column = Report::Column.new(key: column_key, label: column_key.capitalize, type: type)
    # As a view renders it: a plain String is escaped, a SafeBuffer is not.
    Nokogiri::HTML5.fragment(ERB::Util.html_escape(helper.report_cell(nil, column, value)))
  end

  describe "#report_cell for query text" do
    let(:sql) { "select * from users where id = $1 and name = $2 <script>alert(1)</script> " + ("x" * 300) }

    it "puts the whole query, once, in a disclosure that opens to show it" do
      fragment = cell("example", :text, sql)
      span = fragment.at_css("details.query-disclosure > summary > span.query-text")

      expect(span).not_to be_nil
      expect(span.text).to eq(sql)
      expect(span["title"]).to eq(sql)
      expect(fragment.text).to eq(sql)
    end

    it "escapes the query" do
      fragment = cell("example", :text, sql)

      expect(fragment.css("script")).to be_empty
    end

    it "leaves other text columns plain" do
      expect(cell("role", :text, "primary").to_html).to eq("primary")
    end

    it "renders nothing for a missing query" do
      expect(cell("example", :text, nil).to_html).to eq("")
    end
  end
end

RSpec.describe ReportsHelper, "#highlight_match", type: :helper do
  # The highlighted HTML, and the marked spans' text.
  def highlight(text, pattern)
    html = helper.highlight_match(text, pattern)
    expect(html).to be_html_safe
    fragment = Nokogiri::HTML5.fragment(html)
    expect(fragment.text).to eq(text.to_s)
    [html.to_s, fragment.css("mark").map(&:text)]
  end

  it "marks every match, case-insensitively" do
    html, marks = highlight("select Users from users", "USERS")

    expect(marks).to eq(%w[Users users])
    expect(html).to eq("select <mark>Users</mark> from <mark>users</mark>")
  end

  it "escapes the text outside and inside the marks" do
    html, marks = highlight("<b>x</b> & <script>alert(1)</script>", "<script>")

    expect(marks).to eq(["<script>"])
    expect(html).to eq("&lt;b&gt;x&lt;/b&gt; &amp; <mark>&lt;script&gt;</mark>alert(1)&lt;/script&gt;")
  end

  it "never marks inside an escaped entity" do
    html, marks = highlight("a < b", "lt")

    expect(marks).to be_empty
    expect(html).to eq("a &lt; b")
  end

  it "marks non-overlapping matches, leftmost first" do
    expect(highlight("aaaaa", "aa").last).to eq(%w[aa aa])
    expect(highlight("abcabc", "abc|bca").last).to eq(%w[abc abc])
  end

  it "marks nothing for empty matches" do
    html, marks = highlight("select 1", "x*")
    expect(marks).to be_empty
    expect(html).to eq("select 1")

    expect(highlight("baab", "a*").last).to eq(["aa"])
    expect(highlight("abc", "^").last).to be_empty
  end

  it "handles multibyte text" do
    html, marks = highlight("select 'naïve ☃ café' from ünïcode", "ve|CAF|code")

    expect(marks).to eq(%w[ve caf code])
    expect(html).to eq("select &#39;naï<mark>ve</mark> ☃ <mark>caf</mark>é&#39; from ünï<mark>code</mark>")
  end

  it "leaves the text plain without a pattern" do
    expect(highlight("select 1", nil)).to eq(["select 1", []])
    expect(highlight("select 1", "")).to eq(["select 1", []])
    expect(highlight("<i>", nil)).to eq(["&lt;i&gt;", []])
  end

  it "marks matches with the regex syntax Ruby and Postgres share" do
    expect(highlight("users_id, 42", "\\d+").last).to eq(["42"])
    expect(highlight("a.b", "a\\.b").last).to eq(["a.b"])
    expect(highlight("abab", "(ab)\\1").last).to eq(["abab"])
    expect(highlight("foo bar", "[[:alpha:]]+$").last).to eq(["bar"])
    expect(highlight("foo bar", "(?i)BAR").last).to eq(["bar"])
    expect(highlight("foobar", "foo(?=bar)").last).to eq(["foo"])
    expect(highlight("a#b", "[^#]").last).to eq(%w[a b])
    expect(highlight("x]y", "[]x]+").last).to eq(["x]"])
  end

  it "marks nothing, without raising, for a pattern that doesn't compile in Ruby" do
    ["[[:<:]]users", "users[[:>:]]", "(?s)users", "***=users", "(?<name>users", "a{,2}+(", "[z-a]"].each do |pattern|
      expect(highlight("select * from users", pattern)).to eq(["select * from users", []]), pattern
    end
  end

  it "marks nothing for Postgres syntax that Ruby compiles with another meaning" do
    {
      "\\musers" => "users",        # Postgres: start of word. Ruby: the letter m.
      "users\\M" => "usersM",
      "\\yusers\\y" => "y users y", # Postgres: word boundary. Ruby: the letter y.
      "\\Yser" => "users",
      "\\Busers" => "\\users",      # Postgres: a backslash. Ruby: not a word boundary.
      "\\busers" => "users",        # Postgres: backspace. Ruby: word boundary.
      "\\U0001F600" => "U0001F600",
      "\\x41" => "A",
      "\\Z" => "users",
      "(?m)users" => "users",       # Postgres: newline-sensitive. Ruby: dot matches newline.
      "(?x)u s e r s" => "users",
      "[[.a.]]" => ".a",            # Postgres: a collating element. Ruby: a nested class.
      "[[=a=]]" => "=a",
      "[a[bc]]" => "a[b]",          # Ruby: a nested class. Postgres: a literal [.
      "[a-z&&[^x]]" => "&&x"        # Ruby: class intersection. Postgres: literal &.
    }.each do |pattern, text|
      expect(highlight(text, pattern)).to eq([ERB::Util.html_escape(text), []]), pattern
    end
  end

  it "marks nothing when ^ or $ could mean a line, not the text, in Ruby" do
    expect(highlight("a\nb", "^b").last).to be_empty
    expect(highlight("a b", "^a").last).to eq(["a"])
  end

  it "marks nothing where Postgres's longest match would differ from Ruby's first" do
    # Postgres marks <abcd> for both; Ruby's backtracking finds abc, and ab.
    expect(highlight("abcd", "abc|abcd").last).to be_empty
    expect(highlight("abcd", "(ab)?(abcd)?").last).to be_empty
    expect(highlight("abcd abc", "abcd|abc").last).to eq(%w[abcd abc])
    # An empty match where a longer one starts: Postgres marks the a.
    expect(highlight("a", "x*|a").last).to be_empty
  end

  it "marks nothing for lazy, possessive or stacked quantifiers, and for {,n}" do
    ["a+?", "a*?", "a??", "a{1,2}?", "a++", "a*+", "a{,2}", "a{x}", "x{"].each do |pattern|
      expect(highlight("aaa x{", pattern).last).to be_empty, pattern
    end
    expect(highlight("aaaa", "a{2}").last).to eq(%w[aa aa])
    expect(highlight("aaaa", "a{1,3}").last).to eq(%w[aaa a])
  end

  it "marks nothing for locale-dependent classes on non-ASCII text" do
    expect(highlight("naïve", "\\w+").last).to be_empty
    expect(highlight("naïve", "[[:alpha:]]+").last).to be_empty
    expect(highlight("naive", "\\w+").last).to eq(["naive"])
    expect(highlight("[upper]", "[[:upper:]]").last).to be_empty
  end

  it "marks nothing where Ruby's case folding goes beyond Postgres's" do
    expect(highlight("straße", "strasse").last).to be_empty
    expect(highlight("strasse", "straße").last).to be_empty
    expect(highlight("\u212A", "k").last).to be_empty
  end

  # Postgres's case-insensitive matching of non-ASCII characters depends on
  # the database's collation (with COLLATE "C", ü doesn't match Ü), which
  # the UI can't see. So a pattern with any non-ASCII character, literal or
  # escaped, gets no marks.
  it "marks nothing for a pattern with a non-ASCII character, literal or escaped" do
    expect(highlight("Ü users", "ü|users").last).to be_empty
    expect(highlight("Ünïcode", "ü").last).to be_empty
    expect(highlight("ss users", "\\u00df|users").last).to be_empty
    expect(highlight("Ü users", "\\u00fc|users").last).to be_empty
    expect(highlight("A users", "\\u0041|users").last).to be_empty
    expect(highlight("Ü users", "[ü]|users").last).to be_empty
    expect(highlight("Ü users", "users").last).to eq(["users"])
  end

  it "marks nothing for a pattern with an i where the database folds ASCII case its own way" do
    allow(MatchHighlighter).to receive(:ascii_folding_safe?).and_return(false)

    expect(highlight("I users", "i|users").last).to be_empty
    expect(highlight("I users", "users").last).to eq(["users"])
  end

  it "asks about ASCII folding only once there's a pattern" do
    allow(MatchHighlighter).to receive(:ascii_folding_safe?).and_return(true)

    expect(highlight("I users", "i|users").last).to eq(%w[I users])
    helper.highlight_match("I users", "")
    expect(MatchHighlighter).to have_received(:ascii_folding_safe?).once
  end

  it "marks a pattern that's only spaces" do
    expect(highlight("select  x", " ").last).to eq([" ", " "])
    expect(highlight("select  x", "  ").last).to eq(["  "])
  end

  it "keeps a slow pattern within the page's budget, checking it between matches" do
    pattern = "a" + ("(?=.*b)" * 28)
    text = ("a" * 499) + ("b" * 13) + "#" + ("b" * 2200)
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    cells = Array.new(3) { highlight(text, pattern).last }

    expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < MatchHighlighter::BUDGET + 0.15
    # A cell cut short gets no marks rather than some of them.
    cells.each { |marks| expect(marks).to be_empty.or eq(["a"] * 499) }
    expect(cells.last).to be_empty
  end

  it "drops a cell's marks when the budget runs out partway through it" do
    pattern = "a" + ("(?=.*b)" * 28)
    text = ("a" * 400) + ("b" * 13) + "#" + ("b" * 9000)
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    expect(highlight(text, pattern).last).to be_empty
    expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < MatchHighlighter::BUDGET + 0.15
  end

  it "stops highlighting once the page's budget is spent" do
    allow(Process).to receive(:clock_gettime).and_call_original
    clock = 0.0
    # Every reading of the clock moves it on 0.1s: a cell of "users" reads
    # it four times, so the first costs 0.3s and the second runs out.
    allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { clock += 0.1 }

    expect(highlight("users", "users").last).to eq(["users"])
    expect(highlight("users", "users").last).to be_empty
    expect(highlight("users", "users").last).to be_empty
    expect(highlight("users", "other|users").last).to eq(["users"])
  end

  it "gives up, marking nothing, when the pattern takes too long in Ruby" do
    text = "aaaa aaaa aaaa aaaa aaaa aaaa aaaa aaaa aaaa aaaa!"
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    expect(highlight(text, "(\\w+\\s?)*\\1$")).to eq([text, []])
    # Once one cell times out, the page stops trying with that pattern.
    expect(highlight(text, "(\\w+\\s?)*\\1$")).to eq([text, []])

    expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 1
  end
end
