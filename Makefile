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

.PHONY: test test-unit shell image

image:
	docker build -q -f $(DOCKERFILE) -t $(IMAGE) . >/dev/null

## test: all Go tests, race detector on, Docker socket mounted for testcontainers.
test: image
	$(DOCKER_RUN) $(DOCKER_SOCK) $(IMAGE) go test -race $(GO_TEST_ARGS) ./...

## test-unit: Go tests with -short and no Docker socket.
test-unit: image
	$(DOCKER_RUN) $(IMAGE) go test -short $(GO_TEST_ARGS) ./...

## shell: interactive shell in the test container, same mounts as test.
shell: image
	$(DOCKER_RUN) -i $(DOCKER_SOCK) $(IMAGE) bash
