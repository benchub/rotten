# The landing page's heat map, drawn as server-side SVG like the fingerprint
# charts (FingerprintsHelper): tag helpers escape the text, and colors come
# from classes (app/assets/tailwind/reports.css), so the CSP needs no inline
# styles. x is the hour, y is the mean time per call in half-decade buckets
# (see migration 0016), and each cell's shade is the number of fingerprints.
module HomeHelper
  HEATMAP_WIDTH = 760
  HEATMAP_LEFT = 52
  HEATMAP_RIGHT = 8
  HEATMAP_TOP = 8
  HEATMAP_ROW = 12
  HEATMAP_BOTTOM = 22
  HEATMAP_BUCKETS = (-4..10).to_a.freeze
  HEATMAP_LEVELS = 5

  # cells is {[hour, bucket] => fingerprints}. The columns are every hour from
  # the one holding as_of - 7 days through the one holding as_of.
  def home_heatmap(cells, as_of:)
    last = as_of.utc.beginning_of_hour
    first = (as_of.utc - 7.days).beginning_of_hour
    hours = ((last - first) / 3600).round + 1
    plot_height = HEATMAP_BUCKETS.size * HEATMAP_ROW
    height = HEATMAP_TOP + plot_height + HEATMAP_BOTTOM
    column = (HEATMAP_WIDTH - HEATMAP_LEFT - HEATMAP_RIGHT) / hours.to_f
    shown = cells.select { |(hour, bucket), n| hour.between?(first, last) && HEATMAP_BUCKETS.include?(bucket) && n.positive? }
    max = shown.values.max || 0

    parts = [tag.title("Fingerprints by mean duration, last week"),
             tag.desc(heatmap_description(shown, max)),
             tag.rect(x: HEATMAP_LEFT, y: HEATMAP_TOP, width: HEATMAP_WIDTH - HEATMAP_LEFT - HEATMAP_RIGHT,
                      height: plot_height, class: "heat-plot")]
    shown.sort.each do |(hour, bucket), n|
      x = HEATMAP_LEFT + (((hour - first) / 3600).round * column)
      parts << tag.rect(tag.title("#{hour.strftime('%Y-%m-%d %H:%M')} UTC, #{heatmap_band(bucket)}: " \
                                  "#{pluralize(number_with_delimiter(n), 'fingerprint')}"),
                        x: x.round(2), y: heatmap_row_top(bucket), width: column.round(2), height: HEATMAP_ROW,
                        class: "heat-cell heat-#{heatmap_level(n, max)}", data: { fingerprints: n })
    end
    HEATMAP_BUCKETS.select(&:even?).each do |bucket|
      parts << tag.text(heatmap_ms(heatmap_bound(bucket)), x: HEATMAP_LEFT - 4, y: heatmap_row_top(bucket) + HEATMAP_ROW + 3,
                                                           "text-anchor": "end", class: "chart-label")
    end
    (0...hours).each do |i|
      hour = first + i.hours
      next unless hour.hour.zero?

      parts << tag.text(hour.strftime("%b %-d"), x: (HEATMAP_LEFT + (i * column)).round(2), y: height - 6,
                                                 "text-anchor": "middle", class: "chart-label")
    end
    if shown.empty?
      parts << tag.text("No events in the last week", x: HEATMAP_LEFT + ((HEATMAP_WIDTH - HEATMAP_LEFT) / 2.0),
                                                       y: HEATMAP_TOP + (plot_height / 2.0), "text-anchor": "middle",
                                                       class: "chart-empty")
    end
    svg = tag.svg(safe_join(parts), class: "heatmap", role: "img", viewBox: "0 0 #{HEATMAP_WIDTH} #{height}")
    tag.figure(svg + heatmap_legend(max), class: "heatmap-figure")
  end

  private

  def heatmap_row_top(bucket) = HEATMAP_TOP + ((HEATMAP_BUCKETS.last - bucket) * HEATMAP_ROW)

  # Bucket b's lower bound in ms, 10^(b/2).
  def heatmap_bound(bucket) = 10.0**(bucket / 2.0)

  def heatmap_band(bucket)
    return "under #{heatmap_ms(heatmap_bound(bucket + 1))}" if bucket == HEATMAP_BUCKETS.first
    return "#{heatmap_ms(heatmap_bound(bucket))} and up" if bucket == HEATMAP_BUCKETS.last

    "#{heatmap_ms(heatmap_bound(bucket))} to #{heatmap_ms(heatmap_bound(bucket + 1))}"
  end

  def heatmap_ms(ms)
    return "#{number_with_precision(ms / 1000.0, precision: 3, significant: true, strip_insignificant_zeros: true)} s" if ms >= 1000

    "#{number_with_precision(ms, precision: 3, significant: true, strip_insignificant_zeros: true)} ms"
  end

  # Shades on a log scale, so a few busy cells don't wash out the rest:
  # 1 fingerprint is the lightest, max is the darkest.
  def heatmap_level(n, max)
    return HEATMAP_LEVELS if max <= 1

    (1 + ((HEATMAP_LEVELS - 1) * Math.log(n) / Math.log(max))).floor.clamp(1, HEATMAP_LEVELS)
  end

  def heatmap_legend(max)
    return "".html_safe if max.zero?

    swatches = (1..HEATMAP_LEVELS).map { |level| tag.span("", class: "heat-swatch heat-#{level}") }
    tag.figcaption(safe_join(["1 fingerprint", tag.span(safe_join(swatches), class: "heat-scale", "aria-hidden": "true"),
                              pluralize(number_with_delimiter(max), "fingerprint")], " "), class: "heatmap-legend")
  end

  def heatmap_description(shown, max)
    return "No events in the last week." if shown.empty?

    "#{pluralize(shown.size, 'cell')} with fingerprints; the busiest has #{pluralize(number_with_delimiter(max), 'fingerprint')}."
  end
end
