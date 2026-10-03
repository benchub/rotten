FROM golang:1.27 AS build

ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${DATE}" -o /out/rotten-worker ./cmd/rotten-worker

FROM scratch AS worker-bin
COPY --from=build /out/rotten-worker /rotten-worker

FROM debian:stable-slim

RUN set -eux; \
    mkdir -p /etc/rotten-worker /var/lib/rotten-worker /etc/ssl/certs; \
    chown -R 65532:65532 /etc/rotten-worker /var/lib/rotten-worker

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/rotten-worker /usr/local/bin/rotten-worker

USER 65532:65532
ENTRYPOINT ["/usr/local/bin/rotten-worker"]
CMD ["-config", "/etc/rotten-worker/worker.json"]
