module ReportsHelper
  def report_cell(query, column, value)
    return "" if value.nil?

    case column.type
    when :fingerprint
      link_to value.to_s, fingerprint_path(value, query.source_params)
    when :count then number_with_delimiter(value.round)
    when :ms, :percent, :number then number_with_precision(value, precision: 2, delimiter: ",")
    when :time then value.utc.strftime("%Y-%m-%d %H:%M")
    when :context then report_contexts(value)
    else column.key == "example" ? report_query_text(value.to_s) : value.to_s
    end
  end

  # Query text, cut to one line by CSS (see .query-disclosure). The summary
  # holds the whole query once, so screen readers and copying get all of it;
  # opening the disclosure (click, Enter or Space) wraps it to show it all.
  def report_query_text(sql)
    tag.details(tag.summary(tag.span(sql, class: "query-text", title: sql)), class: "query-disclosure")
  end


  def report_contexts(contexts)
    items = Array(contexts).filter_map do |context|
      next unless context.is_a?(Hash)

      tag.li("#{report_context_name(context)} (#{number_with_delimiter(context['times'].to_i)})")
    end
    items.empty? ? "" : tag.ul(safe_join(items), class: "report-contexts")
  end

  # A job tag, or controller#action.
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

  # A tab that runs report on query's dataset.
  def report_tab_link(query, report)
    link_to report.title, reports_path({ report: report.key }.merge(query.switch_params(report))),
            class: "report-tab", aria: { current: ("page" if report == query.report) }
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
