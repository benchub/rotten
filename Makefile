# Go tests run inside docker/test.Dockerfile so they behave the same on macOS
# and Linux. pg_query_go v5 doesn't build natively on macOS yet, so test-unit
# also runs in the container for now (no Docker socket, -short).
#
# Sibling containers: the host Docker socket is mounted, so testcontainers in
# the test container starts containers next to it, not inside it. Their
# published ports live on the Docker host, so we point testcontainers at
# host.docker.internal. Docker Desktop on macOS provides that name; on Linux,
# --add-host=host.docker.internal:host-gateway maps it to the host.
# For a non-default socket: make test DOCKER_SOCK_PATH=$HOME/.colima/default/docker.sock
#
# golden: regenerates testdata/fingerprints.golden. Run it after any change to
# pg_query_go or the normalization code in fingerprint.go, then review
# `git diff testdata/fingerprints.golden` to decide whether the changes are
# intended. Error text is part of the golden output, so changed error messages
# show up in the diff too. Never edit the golden file by hand. It runs as root
# with the same cache volumes as test, then chowns the golden file to your
# host uid:gid so it isn't root-owned on Linux.

IMAGE      ?= rotten-test
DOCKER_SOCK_PATH ?= /var/run/docker.sock
DOCKERFILE := docker/test.Dockerfile

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

.PHONY: test test-unit golden shell image

## image: the Go test image, plus the rotten DB image (Postgres 18 + pg_partman)
## that internal/testdb.StartRotten runs by name.
image:
	docker build -q -f $(DOCKERFILE) -t $(IMAGE) . >/dev/null
	docker build -q -f docker/rotten-db.Dockerfile -t rotten-db-test:18 docker >/dev/null

## test: all Go tests, race detector on, Docker socket mounted for testcontainers.
test: image
	$(DOCKER_RUN) $(DOCKER_SOCK) $(IMAGE) go test -race $(GO_TEST_ARGS) ./...

## test-unit: Go tests with -short and no Docker socket.
test-unit: image
	$(DOCKER_RUN) $(IMAGE) go test -short $(GO_TEST_ARGS) ./...

## golden: regenerate the fingerprint golden file, owned by the host user.
golden: image
	$(DOCKER_RUN) -e HOST_UID="$$(id -u)" -e HOST_GID="$$(id -g)" $(IMAGE) sh -c \
		'go test -short -run "^TestFingerprintGolden$$" -update . && \
		chown "$$HOST_UID:$$HOST_GID" testdata/fingerprints.golden'

## shell: interactive shell in the test container, same mounts as test.
shell: image
	$(DOCKER_RUN) -i $(DOCKER_SOCK) $(IMAGE) bash
