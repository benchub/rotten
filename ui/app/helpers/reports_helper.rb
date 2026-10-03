module ReportsHelper
  def report_cell(query, column, value)
    return "" if value.nil?

    case column.type
    when :fingerprint
      link_to value.to_s, report_path("fingerprint_timeseries", query.source_params.merge(fingerprint_id: value))
    when :count then number_with_delimiter(value.round)
    when :ms, :percent, :number then number_with_precision(value, precision: 2, delimiter: ",")
    when :time then value.utc.strftime("%Y-%m-%d %H:%M")
    when :context then report_contexts(value)
    else value.to_s
    end
  end

  def report_contexts(contexts)
    items = Array(contexts).filter_map do |context|
      next unless context.is_a?(Hash)

      name = context["job_tag"].presence || "#{context['controller']}##{context['action']}"
      tag.li("#{name} (#{number_with_delimiter(context['times'].to_i)})")
    end
    items.empty? ? "" : tag.ul(safe_join(items), class: "report-contexts")
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
    link_to column.label, report_path(query.report.key, query.link_params(sort: column.key, dir: dir))
  end

  def report_aria_sort(query, column)
    return nil unless query.sort_column == column

    query.direction == "asc" ? "ascending" : "descending"
  end
end
