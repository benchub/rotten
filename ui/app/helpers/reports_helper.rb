module ReportsHelper
  # The text columns the match pattern is highlighted in: the query, and the
  # utilization reports' names.
  HIGHLIGHTED_COLUMNS = %w[example controller_action job_tag].freeze

  UNPARSED_TITLE = "Fingerprinted by text: the Postgres 17 parser rejected it".freeze

  # row is the whole result row, for cells that read more than their own
  # column: a fingerprint's unparsed flag.
  def report_cell(query, column, value, row = nil)
    return "" if value.nil?

    pattern = query&.report&.matches? ? query.match_pattern : nil
    case column.type
    when :fingerprint
      link = link_to(value.to_s, fingerprint_path(value, query.source_params))
      row&.dig("unparsed") ? safe_join([link, " ", unparsed_badge]) : link
    when :count then number_with_delimiter(value.round)
    when :ms, :percent, :number then number_with_precision(value, precision: 2, delimiter: ",")
    when :time then value.utc.strftime("%Y-%m-%d %H:%M")
    when :context then report_contexts(value, pattern)
    else
      if column.key == "example" then report_query_text(value.to_s, pattern)
      elsif HIGHLIGHTED_COLUMNS.include?(column.key) then highlight_match(value.to_s, pattern)
      else value.to_s
      end
    end
  end

  # Marks a fallback fingerprint: the worker's parser rejected the statement,
  # so it hashed the pg_stat_statements text instead (docs/worker.md).
  def unparsed_badge
    tag.span("unparsed", class: "unparsed-badge", title: UNPARSED_TITLE)
  end

  # The note under a fingerprint report on how many queries in the window
  # fell back to text fingerprints, or nil when none did. It counts the whole
  # source and window, not only the listed or matching rows.
  def unparsed_summary(summary)
    count = summary && summary["fingerprints"].to_i
    return if count.nil? || count.zero?

    calls = number_with_delimiter(summary["calls"].to_f.round)
    text = if count == 1
             "1 query in this window was fingerprinted by its text, with #{calls} calls: " \
               "the worker's Postgres 17 parser rejected it."
           else
             "#{number_with_delimiter(count)} queries in this window were fingerprinted by their text, with #{calls} calls: " \
               "the worker's Postgres 17 parser rejected them."
           end
    tag.p(text, class: "unparsed-summary")
  end

  # Query text, cut to one line by CSS (see .query-disclosure). The summary
  # holds the whole query once, so screen readers and copying get all of it;
  # opening the disclosure (click, Enter or Space) wraps it to show it all.
  # Matches of pattern are marked in it, open or closed.
  def report_query_text(sql, pattern = nil)
    tag.details(tag.summary(tag.span(highlight_match(sql, pattern), class: "query-text", title: sql)), class: "query-disclosure")
  end

  # The text, HTML-escaped, with each match of the report's match pattern in
  # a <mark>. Matching is MatchHighlighter's, which gives no matches rather
  # than wrong ones where Ruby's regex dialect may differ from Postgres's.
  # One highlighter per pattern serves the whole page, so its time budget
  # and a timeout's give-up span every cell.
  def highlight_match(text, pattern)
    text = text.to_s
    return ERB::Util.html_escape(text) if pattern.nil? || pattern.empty?

    @match_highlighters ||= {}
    spans = (@match_highlighters[pattern] ||= MatchHighlighter.new(pattern, ascii_folding_safe: MatchHighlighter.ascii_folding_safe?)).spans(text)
    parts = []
    at = 0
    spans.each do |from, to|
      parts << text[at...from] << tag.mark(text[from...to])
      at = to
    end
    safe_join(parts << text[at..])
  end

  def report_contexts(contexts, pattern = nil)
    items = Array(contexts).filter_map do |context|
      next unless context.is_a?(Hash)

      tag.li(safe_join([highlight_match(report_context_name(context), pattern),
                        " (#{number_with_delimiter(context['times'].to_i)})"]))
    end
    items.empty? ? "" : tag.ul(safe_join(items), class: "report-contexts")
  end

  # A job tag, or controller#action.
  # pg_stat_statements keeps one text per entry, the first it saw, and the
  # worker credits all the entry's calls to the context in that text. One
  # fingerprint can span several entries, each with its own context.
  CONTEXT_CAVEAT = "Contexts are approximate. Postgres keeps one query text for each pg_stat_statements entry (per " \
                   "user, database and query): the first it saw, which may be from before this time range. All of " \
                   "an entry's calls are credited to the context in that text, so a count means calls of entries " \
                   "first seen under that context, not every call the context made."
  CONTEXT_HEADER_TITLE = "From the first query text of each pg_stat_statements entry, not from each call"

  # The note under a table that shows contexts. Context column headers point
  # at it with aria-describedby.
  def context_caveat
    tag.p(CONTEXT_CAVEAT, id: "context-caveat", class: "context-caveat")
  end

  def report_context_name(context)
    context["job_tag"].presence || "#{context['controller']}##{context['action']}"
  end

  # A sort link for a column header: the first click sorts numbers
  # descending and text ascending, the next click flips it.
  def report_sort_link(query, column)
    current = query.sort_column == column
    dir = if current
            query.direction == "asc" ? "desc" : "asc"
          else
            column.numeric? ? "desc" : "asc"
          end
    link_to column.label, reports_path({ report: query.report.key }.merge(query.link_params(sort: column.key, dir: dir)))
  end

  # The window a valid query runs over, as "2026-10-04 06:35 to 09:35 UTC
  # (last 3 hours)". The end's date is left out when it's the start's.
  def report_window(query)
    from, to = query.window
    to_format = from.to_date == to.to_date ? "%H:%M" : "%Y-%m-%d %H:%M"
    name = query.custom? ? "custom range" : ReportQuery::RANGES.fetch(query.range).first.downcase
    "#{from.strftime('%Y-%m-%d %H:%M')} to #{to.strftime(to_format)} UTC (#{name})"
  end

  # The window's options, each preset with its length for the Custom pre-fill.
  def report_range_options(query)
    choices = ReportQuery::RANGES.map do |key, (label, length)|
      length ? [label, key, { data: { seconds: length.to_i } }] : [label, key]
    end
    options_for_select(choices, query.range)
  end

  # A wrapper for a field only some of reports read, naming them for the
  # report-chooser controller. Nothing renders if none of them reads it.
  # With keep, the field is hidden but still sent for the other reports,
  # as for the dataset role.
  def report_field(reports, field, keep: false, &block)
    keys = reports.select { |report| report.own_fields.include?(field) }.map(&:key)
    return if keys.empty?

    tag.div(class: "field", data: { report_chooser_target: "field", reports: keys.join(" "), report_chooser_keep: (true if keep) }, &block)
  end

  def report_aria_sort(query, column)
    return nil unless query.sort_column == column

    query.direction == "asc" ? "ascending" : "descending"
  end
end
