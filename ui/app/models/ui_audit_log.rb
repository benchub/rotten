# rotten.ui_audit_log: what admins did in the UI. rotten_ui may only insert
# and read it, and the database sets id and at.
class UiAuditLog < ApplicationRecord
  self.table_name = "ui_audit_log"

  INSERT_SQL = Rails.root.join("app/sql/ui_audit_log/insert.sql").read.freeze

  # Who acted, when it wasn't a signed-in user: the bin/rails users:* tasks,
  # or the IdP's groups at OIDC login. They have no users.id.
  SystemActor = Data.define(:id, :email)
  RAKE = SystemActor.new(id: nil, email: "rake")
  OIDC = SystemActor.new(id: nil, email: "oidc")

  # Newest first, by id: id follows insert order, and the primary key index
  # serves the before keyset.
  def self.page(before:, size:)
    scope = order(id: :desc).limit(size)
    before ? scope.where(id: ...before) : scope
  end

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
