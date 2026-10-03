# Dev stack

`dev/docker-compose.yaml` starts the three-network layout from `docs/plan.md`:

| Network | Services |
| --- | --- |
| `observed` | `observed-postgres`, `worker` |
| `edge` | `worker`, `server` |
| `core` | `server`, `rotten-db` |

Start it from the repository root:

```sh
docker compose -f dev/docker-compose.yaml up
```

The stack creates a runtime-only test CA and server certificate in the
`test-certs` named volume. No private keys are committed. The server listens on
`https://localhost:8443`; its container certificate is issued for
`rotten-server`, the Docker network name used by workers, and also for
`localhost` and `127.0.0.1` for host-side checks. To verify with the CA instead
of `curl -k`, copy it out and pass it to curl:

```sh
docker compose -f dev/docker-compose.yaml cp certs:/certs/ca.pem ca.pem
curl --cacert ca.pem https://localhost:8443/healthz
```

`server-migrate` applies the rotten schema to `rotten-db` before the server
starts. The server has an HTTPS `/healthz` healthcheck, and the worker waits for
it to be healthy. `observed-postgres` is Postgres 18 with `pg_stat_statements`
preloaded.

The `worker` service is currently a topology placeholder on `observed` and
`edge`. The worker still writes directly to the rotten DB until task
`20261001-103222-39`, so running the real binary without joining `core` would
fail by design. After that cutover, this service can run `rotten-worker`
against `rotten-server` without changing the network layout.

The topology tests assert isolation by probing container IPs on the Docker
networks. They do not cover host-published ports: anything reachable through
`host.docker.internal` bypasses Docker bridge-network membership by design.

Stop and remove the stack with:

```sh
docker compose -f dev/docker-compose.yaml down
```

Add `-v` to `down` when you also want to remove the generated certificates and
database volumes.

The dev services share named Go module and build-cache volumes. After changing
`go.mod` or `go.sum`, run `docker compose -f dev/docker-compose.yaml down -v`
before the next `up` so the dev image and volumes agree on the module cache.
