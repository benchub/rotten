# rotten.ui_audit_log: what admins did in the UI. rotten_ui may only insert
# and read it, and the database sets id and at.
class UiAuditLog < ApplicationRecord
  self.table_name = "ui_audit_log"

  INSERT_SQL = Rails.root.join("app/sql/ui_audit_log/insert.sql").read.freeze

  def readonly?
    true
  end

  # Records an action by actor. Call it inside the transaction that makes
  # the change, so the change and its record commit together. details must
  # never hold a secret.
  def self.record!(actor:, action:, target_type:, target_id:, details: {})
    lease_connection.exec_query(INSERT_SQL, "UiAuditLog Insert",
                                [actor.id, actor.email, action, target_type, target_id, details.to_json])
  end
end
