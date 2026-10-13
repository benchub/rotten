# One hour and duration bucket of the landing page's heat map, written by
# rotten-server's refresh_home_stats() (migration 0016).
class HomeHeatmapCell < ApplicationRecord
  self.table_name = "home_heatmap"
  self.primary_key = %w[hour duration_bucket]

  def readonly? = true
end
