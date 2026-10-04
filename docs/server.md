# Running rotten-server

`rotten-server` has three subcommands:

| Subcommand | Runs as | What it does |
| --- | --- | --- |
| `rotten-server migrate` | `rotten_owner` | Applies schema migrations and grants. See [database.md](database.md). |
| `rotten-server keys` | `rotten_owner` | Creates, lists and revokes worker pass keys. See [keys.md](keys.md). |
| `rotten-server serve` | `rotten_ingest` | The HTTPS ingest service that workers send harvests to. |

`rotten-server --version` prints the build version, and `rotten-server
--help` prints a usage summary. Each subcommand takes `-h`.

## migrate and keys flags

`serve`'s settings are under [configuration](#configuration). The other
subcommands take only flags and environment variables:

| Command | Flag | Environment variable | Default |
| --- | --- | --- | --- |
| `migrate` | `-dsn` | `ROTTEN_OWNER_DSN` | Required. Connect as `rotten_owner`. |
| `migrate` | `-retention` | `ROTTEN_RETENTION` | `21 days`. See [database.md](database.md#retention). |
| `keys create`, `keys list`, `keys revoke` | `-dsn` | `ROTTEN_ADMIN_DSN` | Required. Connect as `rotten_owner`. |
| `keys create` | `-fqdn` | | None, but the server refuses a key without one, so always pass it. The worker host the key is pinned to. See [keys.md](keys.md). |

A flag wins over its environment variable.

## What serve does

Workers call two Connect RPCs over HTTPS, authenticated with a pass key in an
`Authorization: Bearer` header:

- `Register` creates or reuses the logical and physical source rows for a
  worker.
- `SubmitHarvest` writes one harvest batch in one transaction. A retried
  batch with the same `batch_id` is recognized and not written twice.

The server checks every request before it opens a transaction (see
[limits](#ingest-limits)). Once an hour it prunes old rows from
`ingested_batches`, the table of batch IDs it uses to spot retries.

## TLS

TLS 1.3 is required. There's no plaintext listener and no TLS 1.2 fallback.
Workers authenticate with pass keys, not client certificates.

- `-tls-cert` is a PEM file with the certificate chain, leaf first. Its names
  must include the host name workers use in `ServerURL`.
- `-tls-key` is the matching PEM private key. Make it readable only by the
  server's user.
- Workers must trust the CA that signed the certificate. Give them that CA
  in `ServerCAFile` (see [worker.md](worker.md)). They verify the host name.

Both files must load before the server connects to the database or opens its
listener; if they don't, it exits.

**Rotating the certificate.** Replace both files. The server rereads them
every second, including files replaced by a rename or a symlink swap, and
`SIGHUP` makes it reload at once. It switches only to a pair that parses and
matches. A missing, malformed or mismatched pair is logged and the last good
pair stays in use, and polling keeps retrying. The server doesn't check
expiry or trust; clients do. New connections get the new certificate, and
existing ones keep theirs. TLS session resumption is off, so a reconnecting
client always checks the current certificate.

## Configuration

Settings come from flags, environment variables and an optional JSON config
file. For each setting, an explicit flag wins, then the `ROTTEN_SERVER_*`
environment variable, then the config file, then the default.

| Flag | Config key | Environment variable | Default |
| --- | --- | --- | --- |
| `-config` | | | No config file |
| `-dsn` | `DSN` | `ROTTEN_SERVER_DSN` | Required. Connect as `rotten_ingest`. |
| `-listen` | `Listen` | `ROTTEN_SERVER_LISTEN` | `:8443` |
| `-tls-cert` | `TLSCert` | `ROTTEN_SERVER_TLS_CERT` | Required |
| `-tls-key` | `TLSKey` | `ROTTEN_SERVER_TLS_KEY` | Required |
| `-shutdown-timeout` | `ShutdownTimeout` | `ROTTEN_SERVER_SHUTDOWN_TIMEOUT` | `10` seconds |
| `-health-timeout` | `HealthTimeout` | `ROTTEN_SERVER_HEALTH_TIMEOUT` | `1` second |
| `-failed-auth-burst` | `FailedAuthBurst` | `ROTTEN_SERVER_FAILED_AUTH_BURST` | `5` failed lookups per client |
| `-failed-auth-refill` | `FailedAuthRefill` | `ROTTEN_SERVER_FAILED_AUTH_REFILL` | `10` seconds per token |
| `-global-failed-auth-burst` | `GlobalFailedAuthBurst` | `ROTTEN_SERVER_GLOBAL_FAILED_AUTH_BURST` | `50` failed lookups |
| `-global-failed-auth-refill` | `GlobalFailedAuthRefill` | `ROTTEN_SERVER_GLOBAL_FAILED_AUTH_REFILL` | `1` second per token |

The timeouts and refills are whole seconds. A config file with every key:

```json
{
  "DSN": "postgres://rotten_ingest@db.example.com/rotten?sslmode=verify-full",
  "Listen": ":8443",
  "TLSCert": "/etc/rotten/tls/server-chain.pem",
  "TLSKey": "/etc/rotten/tls/server-key.pem",
  "ShutdownTimeout": 10,
  "HealthTimeout": 1,
  "FailedAuthBurst": 5,
  "FailedAuthRefill": 10,
  "GlobalFailedAuthBurst": 50,
  "GlobalFailedAuthRefill": 1
}
```

```sh
rotten-server serve -config /etc/rotten/server.json
```

Or with no file at all:

```sh
ROTTEN_SERVER_DSN='postgres://rotten_ingest@db.example.com/rotten' \
  rotten-server serve -tls-cert /etc/rotten/tls/server-chain.pem -tls-key /etc/rotten/tls/server-key.pem
```

Keep a password out of the command line: put the DSN in the config file or
the environment, or use a `.pgpass` file.

**Older environment variable names.** When there's no `-config`,
`ROTTEN_INGEST_DSN`, `ROTTEN_LISTEN`, `ROTTEN_TLS_CERT` and `ROTTEN_TLS_KEY`
still work, as fallbacks for the `ROTTEN_SERVER_*` names. With a config
file they're ignored. The server image always passes `-config`, so use the
new names there.

## Health checks and shutdown

`GET /healthz` needs no key. It pings the database, waiting up to
`HealthTimeout`, and answers `200 ok` if the database is reachable or `503
unhealthy` if not, without the error text. Use it for load balancer or
Kubernetes readiness. Liveness is the service manager's job.

`SIGINT` or `SIGTERM` stops accepting connections and waits up to
`ShutdownTimeout` for requests in flight. After that the server cancels them,
closes connections so open transactions roll back, and exits nonzero.

## Failed key lookups

A pass key the server hasn't cached costs a database lookup. To stop junk or
stale keys from turning into a lookup per request, failed lookups are limited
by two token buckets: one per client IP address (IPv6 is grouped by /64) and
one global. A lookup needs a token from both. When either is empty, an
unknown key is refused with `Unauthenticated` without a lookup. Successful
lookups give their tokens back.

At startup the server loads every active key into its cache, so workers can
reconnect after a restart without spending tokens. Valid keys are cached for
30 seconds.

The client IP is the connection's peer address. `X-Forwarded-For` is
ignored. Behind a load balancer every worker shares the balancer's address,
and therefore one per-client bucket; raise `FailedAuthBurst` if that's a
problem.

## Ingest limits

The server refuses invalid `Register` and `SubmitHarvest` requests before it
opens a transaction.

| Input | Limit |
| --- | ---: |
| Request body | 32 MiB |
| Fingerprint aggregates per harvest | 2000 |
| Query context entries per harvest | 2000 |
| Fingerprint string | 128 bytes |
| Normalized query | 8 KiB |
| Context strings | 512 bytes |
| Floating-point metric values | 1e15 ms |
| Harvest window length | 24 hours |
| Harvest window start or end in the future | 5 minutes |
| Register source strings | 255 bytes |
| Register `worker_version` | 128 bytes |

Window times must be in order. Metrics must be finite and non-negative. Text
must be valid UTF-8 without NUL bytes. There's no limit on how old a window
may be, so a worker can replay a long outbox.

## The container image

`docker/server.Dockerfile` builds a distroless image that runs as a non-root
user with `serve -config /etc/rotten/server.json`. Mount the config file and
the TLS files, and expose the listen port. Logs go to stdout. See
[building.md](building.md).
