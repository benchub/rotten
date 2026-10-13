# The landing page's numbers: one row that rotten-server's
# refresh_home_stats() replaces every few minutes (migration 0016). The UI
# only reads it. docs/ui.md ("Landing page") defines each number.
class HomeStats < ApplicationRecord
  self.table_name = "home_stats"
  self.primary_key = "singleton"

  def readonly? = true

  def self.current = first

  # The heat map's cells, as {[hour, bucket] => fingerprints}.
  def heatmap
    HomeHeatmapCell.pluck(:hour, :duration_bucket, :fingerprints)
                   .to_h { |hour, bucket, n| [[hour.utc, bucket], n] }
  end

  def events_per_minute = events_last_hour / 60.0
end
