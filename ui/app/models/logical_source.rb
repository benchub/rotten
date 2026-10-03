# A project/environment/cluster/role that has reported events. Rows are
# written by rotten-server; the UI only reads them. Row 0 is the all-sources
# row fingerprint_stats uses, not a real source.
class LogicalSource < ApplicationRecord
  ALL_SOURCES_ID = 0

  def readonly? = true

  # Every real source as [project, environment, cluster, role], sorted.
  def self.catalog
    where.not(id: ALL_SOURCES_ID).order(:project, :environment, :cluster, :role)
                                 .pluck(:project, :environment, :cluster, :role)
  end
end
