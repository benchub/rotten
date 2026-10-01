# Test image for the Go code. `make test` builds and runs it.
# cgo stays on because pg_query_go compiles libpg_query from C.
FROM golang:1.27

ENV CGO_ENABLED=1 \
    ROTTEN_TEST_IN_DOCKER=1 \
    GOFLAGS=-buildvcs=false

WORKDIR /src

# Prime the module cache in the image. The Makefile also mounts cache volumes,
# so later runs reuse downloads and build output.
COPY go.mod go.sum ./
RUN go mod download
