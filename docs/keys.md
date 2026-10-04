# Pass keys

Every worker authenticates to `rotten-server` with its own pass key. A key
looks like `rotten_<id>_<secret>`. The database stores only
`sha256(secret)`, so a key is shown once, when it's created, and can't be
recovered later.

Each key is pinned to one worker host. The server compares the key's FQDN
with the `FQDN` in the worker's config, ignoring case and a trailing dot,
and refuses `Register` and `SubmitHarvest` if they differ or if the key isn't
pinned. Several keys may be pinned to the same host, which is what makes
rotation possible.

Manage keys from the command line as `rotten_owner`, or from the UI as an
admin. Both work on the same table.

## From the command line

`rotten-server keys` connects with `-dsn`, or `ROTTEN_ADMIN_DSN` if that's
not set. Use a `rotten_owner` DSN. Put the flags before the name.

```sh
export ROTTEN_ADMIN_DSN='postgres://rotten_owner@db.example.com/rotten'

rotten-server keys create --fqdn db1.example.com db1-worker
rotten-server keys list
rotten-server keys revoke db1-worker
```

- `create --fqdn HOST NAME` prints the key once. Names are unique, even
  among revoked keys. Always pass `--fqdn`; the command accepts a key without
  it, but the server refuses such a key. `HOST` gets the same checks as in
  the UI: labels of letters, digits and inner hyphens, at most 63 characters
  each and 253 in all. Wildcards, ports, underscores and non-ASCII
  characters are refused. Surrounding whitespace is trimmed, and the FQDN is
  stored lowercased without a trailing dot, so `--fqdn DB1.example.com.`
  stores `db1.example.com`. Single-label names such as `localhost` and
  dotted IPv4 addresses pass the checks, but the worker's `FQDN` must match.
  Matching ignores case and a trailing dot.
- `list` shows each key's id, name, FQDN, creator, creation time, last use and
  status. It never shows secrets.
- `revoke NAME` revokes a key. Keys are never deleted.

`created_by` and `revoked_by` record the operating system user who ran the
command.

## From the UI

Admins manage keys at `/admin/keys`. **New pass key** asks for a name and the
worker's FQDN, and shows the key once. **Revoke** revokes a key. The UI
checks and normalizes the FQDN exactly as the command line does, and it
records each create and
revoke in `ui_audit_log`. See `ui/README.md` for details.

## Issuing a key to a worker

1. Create a key pinned to the worker's `FQDN` value.
2. Write it to the worker's `PassKeyFile`, alone in the file. Leading and
   trailing whitespace is ignored. Make the file readable only by the
   worker's user.
3. Start the worker.

## Rotating a key

The worker reads `PassKeyFile` only at startup.

1. Create a new key with a new name and the same `--fqdn`. Both keys now
   work.
2. Replace the contents of `PassKeyFile` with the new key.
3. Restart the worker. Check its log for successful sends.
4. Revoke the old key.

## Revoking a key

Revoke it with `rotten-server keys revoke NAME` or in the UI. The server
caches valid keys, so a revoked key keeps working for up to 30 seconds. After
that the worker gets `Unauthenticated`; it keeps its harvests in its outbox
until it's given a working key and restarted, or the outbox fills up.
