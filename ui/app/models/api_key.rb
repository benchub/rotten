# A worker pass key in rotten.api_keys. rotten_ui may read every column but
# secret_hash, insert only name, secret_hash, fqdn and created_by, and update
# only revoked_at and revoked_by (migrations/permissions.sql). So Active
# Record never names secret_hash, the model is read-only, and writes go
# through the SQL in app/sql/api_keys, which internal/auth's Go tests also
# run as rotten_ui against the server's auth.
class ApiKey < ApplicationRecord
  self.ignored_columns += %w[secret_hash]

  CREATE_SQL = Rails.root.join("app/sql/api_keys/create.sql").read.freeze
  REVOKE_SQL = Rails.root.join("app/sql/api_keys/revoke.sql").read.freeze
  AUDIT_TYPE = "api_key".freeze

  def readonly?
    true
  end

  def revoked?
    revoked_at.present?
  end

  # Inserts a key and returns its id. Run inside the caller's transaction.
  def self.insert_key!(name:, secret_hash:, fqdn:, created_by:)
    Integer(lease_connection.select_value(CREATE_SQL, "ApiKey Create", [name, secret_hash, fqdn, created_by]))
  end

  # Revokes the key as actor and audits it, in one transaction. Returns true
  # if this call revoked it, false if it was already revoked, in which case
  # nothing changes.
  def revoke!(actor)
    self.class.transaction do
      revoked = self.class.lease_connection.select_value(REVOKE_SQL, "ApiKey Revoke", [id, actor.email])
      next false if revoked.nil?

      UiAuditLog.record!(actor: actor, action: "api_key.revoke", target_type: AUDIT_TYPE, target_id: id,
                         details: { name: name, fqdn: fqdn })
      true
    end
  end
end
