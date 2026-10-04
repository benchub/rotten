# Building rotten

## Go binaries

You need Go 1.27 or later. The worker uses cgo, because `pg_query_go`
compiles libpg_query from C, so it also needs a C compiler. The server is
pure Go.

```sh
go build ./cmd/rotten-worker
go build ./cmd/rotten-server
```

Native builds work on macOS and Linux. `rotten-worker --version` and
`rotten-server --version` print the version, which comes from git when you
build with `make`.

## Release artifacts

```sh
make build
```

This writes:

- native binaries to `dist/native`;
- Linux amd64 and arm64 binaries to `dist/linux/<arch>`. The server is
  cross-compiled with `CGO_ENABLED=0`. The worker needs a Linux C toolchain,
  so it's built with `docker build --platform`;
- local production images tagged `rotten-worker:local` and
  `rotten-server:local`, for the host's platform.

The Docker builds don't pull, so pull these base images first:

- `golang:1.27` for `linux/amd64` and `linux/arm64`;
- `debian:stable-slim` and `gcr.io/distroless/static-debian12:nonroot` for
  the host's platform.

`make release-images` builds the worker binaries and both production images
for both `linux/amd64` and `linux/arm64`. It needs all three base images for
both platforms. On an arm64 host, for example:

```sh
docker pull --platform linux/amd64 golang:1.27
docker pull --platform linux/amd64 debian:stable-slim
docker pull --platform linux/amd64 gcr.io/distroless/static-debian12:nonroot
```

This repository builds per-platform images and binaries only. Build and push
multi-platform manifests in your deploy repository.

## Images

| Dockerfile | Base | Runs as | Default command |
| --- | --- | --- | --- |
| `docker/worker.Dockerfile` | Debian slim | user `65532` | `rotten-worker -config /etc/rotten-worker/worker.json` |
| `docker/server.Dockerfile` | distroless static | `nonroot` | `rotten-server serve -config /etc/rotten/server.json` |
| `ui/Dockerfile` | Ruby 3.4 slim | user `1000` | Thruster in front of Puma, on port 80 |

Both Go images log to stdout. The UI image needs the `reports` directory as a
named build context. From the repository root:

```sh
docker build --build-context reports=reports -t rotten-ui ui
```

See [ui.md](ui.md) for running it.

## Tests

| Target | What it runs |
| --- | --- |
| `make test-unit` | Go tests in `-short` mode, natively. Tests that need Docker are skipped. |
| `make test` | Every Go test in Docker, with the race detector, against real Postgres 14 through 18 in containers. |
| `make test-ui` | The Rails specs in Docker, in a dev image built for the host's native platform (`UI_PLATFORM` overrides). |
| `make test-all` | `make test` and `make test-ui`. |
| `make test-perf` | The report performance suite. It seeds about 10 million events and takes several minutes. |
| `make test-release` | Builds the release artifacts and smoke-tests them. |
