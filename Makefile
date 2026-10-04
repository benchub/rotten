# Go tests run inside docker/test.Dockerfile so they behave the same on macOS
# and Linux. test-unit runs natively with the host Go toolchain (-short, no
# Docker needed); pg_query_go v6 builds natively on macOS.
#
# Sibling containers: the host Docker socket is mounted, so testcontainers in
# the test container starts containers next to it, not inside it. Their
# published ports live on the Docker host, so we point testcontainers at
# host.docker.internal. Docker Desktop on macOS provides that name; on Linux,
# --add-host=host.docker.internal:host-gateway maps it to the host.
# For a non-default socket: make test DOCKER_SOCK_PATH=$HOME/.colima/default/docker.sock
#
# golden: regenerates internal/fingerprint/testdata/fingerprints.golden. Run it
# after any change to pg_query_go or the normalization code in
# internal/fingerprint/fingerprint.go, then review
# `git diff internal/fingerprint/testdata/fingerprints.golden` to decide whether
# the changes are intended. Error text is part of the golden output, so changed error messages
# show up in the diff too. Never edit the golden file by hand. It runs natively
# on the host, like test-unit, so the file is written as you. No Docker needed.

IMAGE      ?= rotten-test
DOCKER_SOCK_PATH ?= /var/run/docker.sock
DOCKERFILE := docker/test.Dockerfile
DIST_DIR ?= dist
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null)
COMMIT ?= $(shell git rev-parse HEAD)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(BUILD_DATE)
NATIVE_GOOS := $(shell go env GOOS)
NATIVE_GOARCH := $(shell go env GOARCH)
WORKER_IMAGE ?= rotten-worker:local
SERVER_IMAGE ?= rotten-server:local
WORKER_AMD64_IMAGE ?= rotten-worker:local-amd64
WORKER_ARM64_IMAGE ?= rotten-worker:local-arm64
SERVER_AMD64_IMAGE ?= rotten-server:local-amd64
SERVER_ARM64_IMAGE ?= rotten-server:local-arm64
# The UI dev image is built and run for the Docker daemon's native platform,
# so Chromium and RSpec don't run under emulation on arm64 hosts. Set
# UI_PLATFORM (e.g. linux/amd64) to override. The tag includes the platform,
# so switching platforms builds a new image instead of reusing the other one.
UI_PLATFORM ?= linux/$(shell docker version -f '{{.Server.Arch}}' 2>/dev/null)
UI_IMAGE ?= rotten-ui-dev:$(subst /,-,$(UI_PLATFORM))

DOCKER_RUN := docker run --rm -t \
	-v "$(CURDIR)":/src -w /src \
	-v rotten-gomod:/go/pkg/mod \
	-v rotten-gobuild:/root/.cache/go-build

DOCKER_SOCK := \
	-v "$(DOCKER_SOCK_PATH)":/var/run/docker.sock \
	--add-host=host.docker.internal:host-gateway \
	-e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal \
	-e TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock

GO_TEST_ARGS ?=
# make test's go test timeout. go test's 10-minute default is too short for
# internal/ingest when Docker is busy. A -timeout in GO_TEST_ARGS comes later
# on the command line, so it wins.
GO_TEST_TIMEOUT ?= 30m

.PHONY: test test-unit test-ui test-perf test-all test-release golden shell image ui-image ui-image-check proto tools build build-native build-linux build-linux-smoke build-smoke build-images release-images

# buf is pinned at BUF_VERSION and stays out of go.mod (its dependency tree
# is large). `make tools` installs it into ./bin with GOBIN, and `make proto`
# uses that binary when it's there. Otherwise `make proto` falls back to
# `go run`, which needs network access the first time to download buf's
# modules, then works from the module cache. Neither needs Docker. The
# protoc plugins run through `go tool`, pinned in go.mod's tool block.
BUF_VERSION ?= v1.72.0
BUF := $(if $(wildcard $(CURDIR)/bin/buf),$(CURDIR)/bin/buf,go run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION))
# The breaking-change baseline is where this branch forked from master, so
# work landing on master meanwhile doesn't count against this branch. It
# falls back to master when there's no merge base.
PROTO_BASE ?= $(or $(shell git merge-base HEAD master 2>/dev/null),master)

## tools: install the pinned buf into ./bin (needs network access once).
tools:
	GOBIN="$(CURDIR)/bin" go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)

## proto: regenerate gen/ from proto/, lint, and check for breaking changes
## against $(PROTO_BASE). Breaking is skipped when $(PROTO_BASE) has no
## proto/ yet (the first commit) or doesn't resolve. The baseline is
## exported with git archive, which also works in worktrees, where .git is a
## file.
proto:
	$(BUF) generate
	$(BUF) lint
	@if git cat-file -e "$(PROTO_BASE):proto" 2>/dev/null; then \
		base=$$(mktemp -d) && \
		git archive "$(PROTO_BASE)" proto buf.yaml | tar -x -C "$$base" && \
		$(BUF) breaking --against "$$base"; status=$$?; \
		rm -rf "$$base"; exit $$status; \
	else \
		echo "buf breaking: $(PROTO_BASE) has no proto/ yet, skipping"; \
	fi

## image: the Go test image, plus the rotten DB image (Postgres 18 + pg_partman)
## that internal/testdb.StartRotten runs by name.
image:
	docker build --pull=false -q -f $(DOCKERFILE) -t $(IMAGE) . >/dev/null
	docker build --pull=false -q -f docker/rotten-db.Dockerfile -t rotten-db-test:18 docker >/dev/null

## ui-image: the Rails development/test image with Chromium for system specs.
ui-image:
	docker build --pull=false --platform $(UI_PLATFORM) -q -f ui/dev.Dockerfile -t $(UI_IMAGE) ui >/dev/null
	@$(MAKE) --no-print-directory ui-image-check

## ui-image-check: fail unless $(UI_IMAGE) was built for $(UI_PLATFORM).
ui-image-check:
	@want="$(word 2,$(subst /, ,$(UI_PLATFORM)))"; \
	got="$$(docker image inspect -f '{{.Architecture}}' $(UI_IMAGE))"; \
	if [ -z "$$want" ] || [ "$$got" != "$$want" ]; then \
		echo "ui-image: $(UI_IMAGE) is $$got, want $$want ($(UI_PLATFORM))" >&2; \
		exit 1; \
	fi

## build: native binaries, Linux amd64/arm64 binaries, and local production images.
build: build-native build-linux build-images

## build-smoke: release smoke build using only the native Docker platform.
build-smoke: build-native build-linux-smoke build-images

## build-native: build binaries for this host into dist/native.
build-native:
	mkdir -p "$(DIST_DIR)/native"
	CGO_ENABLED=1 go build -ldflags "$(LDFLAGS)" -o "$(DIST_DIR)/native/rotten-worker" ./cmd/rotten-worker
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o "$(DIST_DIR)/native/rotten-server" ./cmd/rotten-server
	printf '%s\n' "$(NATIVE_GOOS)/$(NATIVE_GOARCH)" > "$(DIST_DIR)/native/platform.txt"

## build-linux: build Linux amd64 and arm64 binaries. The cgo worker is built in Docker per platform.
build-linux:
	rm -rf "$(DIST_DIR)/linux"
	mkdir -p "$(DIST_DIR)/linux/amd64" "$(DIST_DIR)/linux/arm64"
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o "$(DIST_DIR)/linux/amd64/rotten-server" ./cmd/rotten-server
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o "$(DIST_DIR)/linux/arm64/rotten-server" ./cmd/rotten-server
	docker build --pull=false --platform linux/amd64 --target worker-bin --build-arg VERSION="$(VERSION)" --build-arg COMMIT="$(COMMIT)" --build-arg DATE="$(BUILD_DATE)" --output type=local,dest="$(DIST_DIR)/linux/amd64" -f docker/worker.Dockerfile .
	docker build --pull=false --platform linux/arm64 --target worker-bin --build-arg VERSION="$(VERSION)" --build-arg COMMIT="$(COMMIT)" --build-arg DATE="$(BUILD_DATE)" --output type=local,dest="$(DIST_DIR)/linux/arm64" -f docker/worker.Dockerfile .

## build-linux-smoke: build Linux server binaries and the cgo worker for the native Docker platform.
build-linux-smoke:
	rm -rf "$(DIST_DIR)/linux"
	mkdir -p "$(DIST_DIR)/linux/amd64" "$(DIST_DIR)/linux/arm64"
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o "$(DIST_DIR)/linux/amd64/rotten-server" ./cmd/rotten-server
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o "$(DIST_DIR)/linux/arm64/rotten-server" ./cmd/rotten-server
	docker build --pull=false --target worker-bin --build-arg VERSION="$(VERSION)" --build-arg COMMIT="$(COMMIT)" --build-arg DATE="$(BUILD_DATE)" --output type=local,dest="$(DIST_DIR)/linux/$(NATIVE_GOARCH)" -f docker/worker.Dockerfile .

## build-images: build local production images for the host Docker platform.
build-images:
	docker build --pull=false -q -f docker/worker.Dockerfile --build-arg VERSION="$(VERSION)" --build-arg COMMIT="$(COMMIT)" --build-arg DATE="$(BUILD_DATE)" -t "$(WORKER_IMAGE)" . >/dev/null
	docker build --pull=false -q -f docker/server.Dockerfile --build-arg VERSION="$(VERSION)" --build-arg COMMIT="$(COMMIT)" --build-arg DATE="$(BUILD_DATE)" -t "$(SERVER_IMAGE)" . >/dev/null

## release-images: build per-platform production images and cgo worker binaries for Linux amd64 and arm64.
release-images:
	mkdir -p "$(DIST_DIR)/linux/amd64" "$(DIST_DIR)/linux/arm64"
	docker build --pull=false --platform linux/amd64 --target worker-bin --build-arg VERSION="$(VERSION)" --build-arg COMMIT="$(COMMIT)" --build-arg DATE="$(BUILD_DATE)" --output type=local,dest="$(DIST_DIR)/linux/amd64" -f docker/worker.Dockerfile .
	docker build --pull=false --platform linux/arm64 --target worker-bin --build-arg VERSION="$(VERSION)" --build-arg COMMIT="$(COMMIT)" --build-arg DATE="$(BUILD_DATE)" --output type=local,dest="$(DIST_DIR)/linux/arm64" -f docker/worker.Dockerfile .
	docker build --pull=false --platform linux/amd64 -f docker/worker.Dockerfile --build-arg VERSION="$(VERSION)" --build-arg COMMIT="$(COMMIT)" --build-arg DATE="$(BUILD_DATE)" -t "$(WORKER_AMD64_IMAGE)" .
	docker build --pull=false --platform linux/arm64 -f docker/worker.Dockerfile --build-arg VERSION="$(VERSION)" --build-arg COMMIT="$(COMMIT)" --build-arg DATE="$(BUILD_DATE)" -t "$(WORKER_ARM64_IMAGE)" .
	docker build --pull=false --platform linux/amd64 -f docker/server.Dockerfile --build-arg VERSION="$(VERSION)" --build-arg COMMIT="$(COMMIT)" --build-arg DATE="$(BUILD_DATE)" -t "$(SERVER_AMD64_IMAGE)" .
	docker build --pull=false --platform linux/arm64 -f docker/server.Dockerfile --build-arg VERSION="$(VERSION)" --build-arg COMMIT="$(COMMIT)" --build-arg DATE="$(BUILD_DATE)" -t "$(SERVER_ARM64_IMAGE)" .

## test-release: build release artifacts and smoke-test their CLIs and migrations.
test-release:
	ROTTEN_RELEASE_SMOKE=1 go test $(GO_TEST_ARGS) -run '^TestReleaseArtifactsSmoke$$' ./internal/release

## test: all Go tests, race detector on, Docker socket mounted for testcontainers.
## The vet line compiles the perf suite (build tag perf) without running it.
test: image
	$(DOCKER_RUN) $(IMAGE) go vet -tags perf ./reports
	$(DOCKER_RUN) $(DOCKER_SOCK) $(IMAGE) go test -race -timeout $(GO_TEST_TIMEOUT) $(GO_TEST_ARGS) ./...

## test-perf: the report performance suite (reports/perf_test.go, build tag
## perf). It seeds about 10 million events, so it takes several minutes and
## isn't part of test or test-all. ROTTEN_PERF_EVENTS overrides the count.
## Results are recorded in docs/perf.md.
PERF_TEST_ARGS ?=
test-perf: image
	$(DOCKER_RUN) $(DOCKER_SOCK) -e ROTTEN_PERF_EVENTS $(IMAGE) go test -tags perf -run '^TestPerf' -count=1 -timeout 90m -v $(PERF_TEST_ARGS) ./reports

## test-ui: Rails specs in Docker, against a migrated rotten test database.
test-ui: image ui-image
	@set -eu; \
	net="rotten-ui-test-core-$$(date +%s)-$$$$"; \
	db="rotten-ui-test-db-$$(date +%s)-$$$$"; \
	cleanup() { docker rm -f "$$db" >/dev/null 2>&1 || true; docker network rm "$$net" >/dev/null 2>&1 || true; }; \
	trap cleanup EXIT; \
	docker network create "$$net" >/dev/null; \
	docker run --rm --network "$$net" postgres:18 true >/dev/null; \
	docker run -d --name "$$db" --network "$$net" -e POSTGRES_DB=rotten -e POSTGRES_PASSWORD=postgres -v "$(CURDIR)/dev/rotten-db-init.sql":/docker-entrypoint-initdb.d/001-rotten.sql:ro rotten-db-test:18 >/dev/null; \
	ready=0; \
	for attempt in $$(seq 1 60); do \
		if [ "$$(docker inspect -f '{{.State.Running}}' "$$db" 2>/dev/null || true)" != "true" ]; then \
			echo "Postgres container $$db stopped before becoming ready" >&2; \
			docker logs "$$db" >&2 || true; \
			exit 1; \
		fi; \
		if docker run --rm --network "$$net" postgres:18 pg_isready -h "$$db" -U postgres -d rotten >/dev/null 2>&1; then ready=1; break; fi; \
		sleep 1; \
	done; \
	if [ "$$ready" != "1" ]; then \
		echo "Postgres container $$db did not become ready after 60 attempts" >&2; \
		docker logs "$$db" >&2 || true; \
		exit 1; \
	fi; \
	$(DOCKER_RUN) --network "$$net" $(IMAGE) go run ./cmd/rotten-server migrate -dsn "postgres://rotten_owner:rotten_owner@$$db:5432/rotten?sslmode=disable"; \
	docker run --rm -t --platform $(UI_PLATFORM) --network "$$net" -v "$(CURDIR)/ui":/app -v "$(CURDIR)/reports":/reports:ro -w /app -e RAILS_ENV=test -e DATABASE_URL="postgres://rotten_ui:rotten_ui@$$db:5432/rotten?sslmode=disable" -e ROTTEN_UI_TEST_SEED_DATABASE_URL="postgres://rotten_owner:rotten_owner@$$db:5432/rotten?sslmode=disable" -e SECRET_KEY_BASE=test -e ROTTEN_UI_AUTH=password $(UI_IMAGE) bundle exec rspec $(UI_SPEC_ARGS)

## test-all: Go and UI test suites.
test-all: test test-ui

## test-unit: Go tests with -short, run natively on the host. No Docker.
test-unit:
	go test -short $(GO_TEST_ARGS) ./...

## golden: regenerate the fingerprint golden file, run natively on the host.
golden:
	go test -short -run '^TestFingerprintGolden$$' ./internal/fingerprint -update

## shell: interactive shell in the test container, same mounts as test.
shell: image
	$(DOCKER_RUN) -i $(DOCKER_SOCK) $(IMAGE) bash
