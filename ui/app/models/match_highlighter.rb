# Finds where a report's match pattern matched in a piece of text, for
# <mark> highlights. Postgres did the filtering with its own regex engine
# (~*); this replays the pattern in Ruby, which speaks a different dialect.
# So it's conservative: a pattern is only replayed if it uses syntax both
# engines read the same way, and any doubt about a text gives no spans,
# never wrong ones.
#
# - Only a whitelist of syntax is accepted (see #parse). Anything else, such
#   as Postgres's \m, \y or [[:<:]] or Ruby's \b, \h or (?<name>), means no
#   highlights. So does a pattern Ruby can't compile.
# - Postgres picks the longest match at the leftmost position; Ruby picks the
#   first its backtracking finds. If a longer match exists where Ruby matched,
#   the text gets no spans. Lazy quantifiers, which change Postgres's rule,
#   aren't accepted.
# - Ruby's ^ and $ also match at line breaks; Postgres's don't. A pattern with
#   them gets no spans on text with a newline.
# - How Postgres matches non-ASCII characters case-insensitively depends on
#   the database's collation (with COLLATE "C", ü doesn't match Ü), which
#   isn't visible here. So a pattern with any non-ASCII character, literal or
#   as a \u escape, gets no spans.
# - In a Turkish or Azeri locale, Postgres folds I to ı and i to İ, not to
#   each other. If the database or a filtered column uses one (see
#   .ascii_folding_safe?), a pattern with an i, I or bracket expression gets
#   no spans.
# - \w, \s, \d and [:classes:] depend on the database's locale for non-ASCII
#   characters, and Ruby's case folding maps some characters, such as the
#   Kelvin sign to k, where Postgres may not. Those get no spans on non-ASCII
#   text.
# - Every Ruby match runs under Regexp timeout MATCH_TIMEOUT, and one
#   highlighter spends at most BUDGET in total, checked before every match
#   and capping each match's timeout. Past either, the cell at hand gets no
#   spans and the highlighter gives up on the rest of the page.
class MatchHighlighter
  MATCH_TIMEOUT = 0.1
  BUDGET = 0.5
  MAX_TEXT_LENGTH = 10_000
  MAX_MATCHES = 500
  POSIX_CLASSES = %w[alnum alpha blank cntrl digit graph print punct space xdigit].freeze
  CLASS_ESCAPES = %w[d s w D S W].freeze
  CHAR_ESCAPES = %w[n r t f v a e].freeze

  # The columns the report SQL matches with ~*. One with its own collation
  # folds case by that collation, not the database's.
  FILTERED_COLUMNS = { "fingerprints" => "normalized", "controllers" => "controller", "actions" => "action",
                       "job_tags" => "job_tag" }.freeze
  # tr_TR.UTF-8, az_AZ, ICU tr-TR, az-Latn, Windows Turkish_Turkey.1254 ...
  DOTTED_I_LOCALE = /\A(?:tr|az|turkish|azeri)(?:[-_.@]|\z)/i

  @lock = Mutex.new

  class << self
    # Whether the database folds ASCII case as Ruby does (I to i), so a
    # pattern with an i can be replayed. Looked up once per process; a
    # failed lookup counts as unsafe and is tried again next time.
    def ascii_folding_safe?
      @lock.synchronize do
        return @ascii_folding_safe unless @ascii_folding_safe.nil?

        @ascii_folding_safe = ApplicationRecord.with_connection { |conn| lookup_ascii_folding_safe(conn) }
      end
    rescue ActiveRecord::ActiveRecordError, PG::Error
      false
    end

    def forget_ascii_folding = @lock.synchronize { @ascii_folding_safe = nil }

    # Reads the database's locale, and that of any filtered column with a
    # collation of its own, from the catalogs. to_jsonb keeps it to one
    # query across PG 14 (no datlocprovider), 15 and 16 (daticulocale) and
    # 17+ (datlocale).
    def lookup_ascii_folding_safe(conn)
      database = JSON.parse(conn.select_value(<<~SQL, "Match locale"))
        select (to_jsonb(d) - 'datacl')::text from pg_database d where d.datname = current_database()
      SQL
      tables = FILTERED_COLUMNS.keys.map { |table| conn.quote("rotten.#{table}") }.join(", ")
      columns = FILTERED_COLUMNS.values.map { |column| conn.quote(column) }.join(", ")
      collations = conn.select_values(<<~SQL, "Match locale").map { |row| JSON.parse(row) }
        select to_jsonb(c)::text
        from pg_attribute a join pg_collation c on c.oid = a.attcollation
        where a.attrelid in (select to_regclass(t) from unnest(array[#{tables}]) t)
          and a.attname in (#{columns})
          and c.collname <> 'default'
      SQL
      ascii_folding_safe_for?(database, collations)
    end

    # database: a pg_database row; collations: pg_collation rows.
    def ascii_folding_safe_for?(database, collations)
      locales = database.values_at("datcollate", "datctype", "daticulocale", "datlocale") +
                collations.flat_map { |row| row.values_at("collcollate", "collctype", "colliculocale", "colllocale") }
      locales.none? { |locale| locale.is_a?(String) && locale.match?(DOTTED_I_LOCALE) }
    end
  end

  def initialize(pattern, ascii_folding_safe: true)
    @pattern = pattern
    @spent = 0.0
    return unless pattern.is_a?(String) && pattern.ascii_only?
    return if !ascii_folding_safe && pattern.match?(/[iI\[]/)

    @regexp = compile if parse(pattern)
  end

  # Each match's [start, end) character offsets, in order and not
  # overlapping, with empty matches left out. Empty when the text can't be
  # highlighted faithfully.
  def spans(text)
    return [] unless usable?(text)

    started = now
    @deadline = started + BUDGET - @spent
    find_spans(text) || []
  rescue Regexp::TimeoutError, BudgetSpent
    @regexp = nil
    []
  ensure
    @spent += now - started if started
    @regexp = nil if @spent >= BUDGET
  end

  private

  class BudgetSpent < StandardError; end

  def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)

  # The Regexp timeout for the next match: MATCH_TIMEOUT, or what's left of
  # the budget if that's less. Raises BudgetSpent once nothing is left.
  def timeout
    left = @deadline - now
    raise BudgetSpent unless left.positive?

    [left, MATCH_TIMEOUT].min
  end

  # @regexp, or a copy whose timeout fits in what's left of the budget.
  def budgeted_regexp
    left = timeout
    left < MATCH_TIMEOUT ? Regexp.new(@regexp.source, @regexp.options, timeout: left) : @regexp
  end

  def usable?(text)
    return false unless @regexp && text.is_a?(String) && text.valid_encoding? && text.length <= MAX_TEXT_LENGTH
    return false if @anchors && text.include?("\n")
    return false if @classes && !text.ascii_only?

    !fold_hazard?(text)
  end

  def find_spans(text)
    spans = []
    position = 0
    tries = 0
    while position <= text.length && (match = budgeted_regexp.match(text, position))
      return nil if (tries += 1) > MAX_MATCHES

      from, to = match.offset(0)
      return nil if longer_match?(text, from, to)

      if to > from
        spans << [from, to]
        position = to
      else
        position = to + 1
      end
    end
    spans
  end

  # Whether the pattern also matches at from, ending after to: then Postgres's
  # leftmost-longest match isn't the one Ruby found.
  def longer_match?(text, from, to)
    after = text.length - to - 1
    return false if after.negative?

    longer = Regexp.new("\\G(?:#{@regexp.source})(?=[\\s\\S]{0,#{after}}\\z)", @regexp.options, timeout: timeout)
    longer.match?(text, from)
  end

  # Non-ASCII characters whose case folding Ruby and Postgres can disagree on:
  # ones that fold to several characters, to ASCII, or other than they
  # downcase.
  def fold_hazard?(string)
    return false if string.ascii_only?

    string.each_char.any? do |char|
      next false if char.ascii_only?

      folded = char.downcase(:fold)
      folded.length > 1 || folded.ascii_only? || folded != char.downcase
    end
  end

  def compile
    Regexp.new(@pattern, Regexp::IGNORECASE | Regexp::MULTILINE, timeout: MATCH_TIMEOUT)
  rescue RegexpError, ArgumentError, EncodingError
    nil
  end

  # Walks the pattern, accepting only syntax Postgres's ARE and Ruby's
  # Onigmo read the same way. Notes whether it uses ^ or $ (@anchors) and
  # locale-dependent classes (@classes).
  def parse(pattern)
    return false if pattern.start_with?("***")

    @chars = pattern.chars
    @at = 0
    last = :none
    while (char = @chars[@at])
      last = case char
             when "\\" then escape(in_bracket: false) or return false
             when "[" then bracket or return false
             when "(" then group or return false
             when ")" then (@at += 1) && :atom
             when "|" then (@at += 1) && :none
             when "^", "$" then (@anchors = true) && (@at += 1) && :none
             when "*", "+", "?" then quantifier(last, 1) or return false
             when "{" then interval(last) or return false
             else (@at += 1) && :atom
             end
    end
    true
  end

  def quantifier(last, length)
    # Stacked quantifiers are lazy or possessive in Ruby.
    return nil unless last == :atom

    @at += length
    :quantifier
  end

  # Only {m}, {m,} and {m,n}. Postgres reads {,n} as text; Ruby as {0,n}.
  def interval(last)
    found = @chars[@at..].join[/\A\{\d{1,3}(?:,\d{0,3})?\}/]
    found && quantifier(last, found.length)
  end

  def group
    return (@at += 1) && :none unless @chars[@at + 1] == "?"

    rest = @chars[(@at + 2)..].join
    if rest.start_with?(":", "=", "!") then @at += 3
    elsif rest.start_with?("<=", "<!") then @at += 4
    elsif rest.start_with?("i)") then @at += 4
    elsif rest.start_with?("#")
      close = @chars.index(")", @at) or return nil
      @at = close + 1
    else return nil
    end
    :none
  end

  def escape(in_bracket:)
    char = @chars[@at + 1]
    return nil unless char

    @at += 2
    if char.match?(/\A[[:punct:] ]\z/) && char.ascii_only?
      :atom
    elsif CLASS_ESCAPES.include?(char)
      # \D, \S and \W in brackets are an error in some Postgres versions.
      return nil if in_bracket && char.match?(/[[:upper:]]/)

      @classes = true
      :atom
    elsif CHAR_ESCAPES.include?(char)
      :atom
    elsif char == "A" && !in_bracket
      :none
    elsif char.match?(/\A[1-9]\z/) && !in_bracket
      # A one-digit backreference; \10 and up differ.
      @chars[@at]&.match?(/\d/) ? nil : :atom
    end
  end

  def bracket
    @at += 1
    @at += 1 if @chars[@at] == "^"
    @at += 1 if @chars[@at] == "]"
    while (char = @chars[@at])
      case char
      when "]" then return (@at += 1) && :atom
      when "\\" then escape(in_bracket: true) or return nil
      when "[" then posix_class or return nil
      when "&" then @chars[@at + 1] == "&" ? (return nil) : @at += 1
      else @at += 1
      end
    end
    nil
  end

  # [:alpha:] and the like. Ruby reads any other [ in a bracket as a nested
  # class; Postgres reads [. .] and [= =] as collating elements.
  def posix_class
    name = @chars[@at..].join[/\A\[:([a-z]+):\]/, 1]
    return nil unless POSIX_CLASSES.include?(name)

    @classes = true
    @at += name.length + 4
  end
end
