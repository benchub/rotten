FROM golang:1.27 AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${DATE}" -o /out/rotten-server ./cmd/rotten-server

FROM scratch AS server-bin
COPY --from=build /out/rotten-server /rotten-server

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/rotten-server /rotten-server

USER nonroot:nonroot
ENTRYPOINT ["/rotten-server"]
CMD ["serve", "-config", "/etc/rotten/server.json"]
