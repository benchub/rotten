-- Run by the UI as rotten_ui when an admin creates a pass key (ApiKeyIssue).
-- internal/auth/ui_contract_test.go runs this file too, as rotten_ui, so the
-- server's auth is tested against exactly what the UI writes.
-- $1 name, $2 hex(sha256(secret)), $3 fqdn, $4 created_by.
insert into rotten.api_keys (name, secret_hash, fqdn, created_by)
values ($1, $2, $3, $4)
returning id
