# Server-drawn SVG charts for the fingerprint page. Everything goes through
# tag helpers, so text is escaped, and styling is by class (see reports.css),
# so the strict CSP needs no inline styles. Each point carries its bucket
# start and value in data attributes for later hover tooltips.
module FingerprintsHelper
  CHART_WIDTH = 720
  CHART_HEIGHT = 200
  CHART_LEFT = 72
  CHART_RIGHT = 16
  CHART_TOP = 12
  CHART_BOTTOM = 28
  CHART_POINT_RADIUS = 2.5

  # rows are time series rows with a "bucket_start" Time and a numeric
  # value under value_key. They're drawn in time order whatever order they
  # come in.
  def timeseries_chart(rows, value_key:, label:)
    rows = rows.sort_by { |row| row["bucket_start"] }
    values = rows.map { |row| chart_value(row[value_key]) }
    max = values.max || 0.0
    points = rows.each_with_index.map do |row, i|
      [chart_x(i, rows.size), chart_y(values[i], max), row]
    end

    parts = [tag.title(label), tag.desc(chart_description(rows, max)), *chart_axes(rows, max)]
    if points.empty?
      parts << tag.text("No data", x: CHART_LEFT + (chart_plot_width / 2.0), y: CHART_TOP + (chart_plot_height / 2.0),
                                   "text-anchor": "middle", class: "chart-empty")
    end
    parts << tag.polyline(points: points.map { |x, y, _| "#{x},#{y}" }.join(" "), class: "chart-line") if points.size > 1
    points.each do |x, y, row|
      parts << tag.circle(cx: x, cy: y, r: CHART_POINT_RADIUS, class: "chart-point",
                          data: { time: row["bucket_start"].utc.iso8601, value: row[value_key].to_s })
    end

    tag.svg(safe_join(parts), class: "timeseries-chart", role: "img", viewBox: "0 0 #{CHART_WIDTH} #{CHART_HEIGHT}",
                              data: { series: value_key })
  end

  private

  def chart_plot_width = CHART_WIDTH - CHART_LEFT - CHART_RIGHT
  def chart_plot_height = CHART_HEIGHT - CHART_TOP - CHART_BOTTOM
  def chart_baseline = CHART_HEIGHT - CHART_BOTTOM

  def chart_value(value)
    number = value.to_f
    number.finite? && number.positive? ? number : 0.0
  end

  # A single point sits in the middle.
  def chart_x(index, count)
    offset = count > 1 ? chart_plot_width * index / (count - 1).to_f : chart_plot_width / 2.0
    (CHART_LEFT + offset).round(1)
  end

  # With every value zero there's no scale, so points sit on the baseline.
  def chart_y(value, max)
    return chart_baseline.to_f if max.zero?

    (chart_baseline - (chart_plot_height * value / max)).round(1)
  end

  def chart_axes(rows, max)
    right = CHART_WIDTH - CHART_RIGHT
    label_y = CHART_HEIGHT - 8
    axes = [
      tag.line(x1: CHART_LEFT, y1: CHART_TOP, x2: CHART_LEFT, y2: chart_baseline, class: "chart-axis"),
      tag.line(x1: CHART_LEFT, y1: chart_baseline, x2: right, y2: chart_baseline, class: "chart-axis"),
      tag.text(chart_number(max), x: CHART_LEFT - 6, y: CHART_TOP + 4, "text-anchor": "end", class: "chart-label"),
      tag.text("0", x: CHART_LEFT - 6, y: chart_baseline, "text-anchor": "end", class: "chart-label")
    ]
    return axes if rows.empty?

    axes << tag.text(chart_time(rows.first), x: CHART_LEFT, y: label_y, "text-anchor": "start", class: "chart-label")
    if rows.size > 1
      axes << tag.text(chart_time(rows.last), x: right, y: label_y, "text-anchor": "end", class: "chart-label")
    end
    axes
  end

  def chart_description(rows, max)
    return "No data." if rows.empty?

    "#{pluralize(rows.size, 'bucket')} from #{chart_time(rows.first)} to #{chart_time(rows.last)} UTC, " \
      "highest #{chart_number(max)}."
  end

  def chart_time(row) = row["bucket_start"].utc.strftime("%Y-%m-%d %H:%M")

  def chart_number(value) = number_with_precision(value, precision: 2, strip_insignificant_zeros: true, delimiter: ",")
end
