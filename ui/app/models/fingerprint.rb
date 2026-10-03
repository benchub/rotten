# One normalized query. Rows are written by rotten-server; the UI only reads
# them. normalized comes from the observed databases, so views must treat it
# as untrusted text.
class Fingerprint < ApplicationRecord
  ID_FORMAT = /\A[1-9]\d{0,18}\z/

  def readonly? = true

  # The fingerprint with this id, or nil for an id that isn't a positive
  # bigint written in plain decimal digits, or that no fingerprint has.
  def self.lookup(id)
    return nil unless id.is_a?(String) && id.match?(ID_FORMAT)

    value = Integer(id, 10)
    return nil if value > ReportQuery::MAX_FINGERPRINT_ID

    find_by(id: value)
  end
end
