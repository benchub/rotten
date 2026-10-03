-- Run by the UI as rotten_ui when an admin revokes a pass key (ApiKey#revoke!).
-- internal/auth/ui_contract_test.go runs this file too. A key that's already
-- revoked matches no row, so revoking twice changes nothing.
-- $1 key id, $2 revoked_by.
update rotten.api_keys
set revoked_at = now(), revoked_by = $2
where id = $1 and revoked_at is null
returning id
