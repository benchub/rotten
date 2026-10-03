-- Run by the UI as rotten_ui, in the same transaction as the change it
-- records (UiAuditLog.record!). internal/auth/ui_contract_test.go runs it too.
-- $1 actor user id, $2 actor email, $3 action, $4 target type, $5 target id,
-- $6 details as JSON text.
insert into rotten.ui_audit_log (actor_user_id, actor_email, action, target_type, target_id, details)
values ($1, $2, $3, $4, $5, $6::jsonb)
