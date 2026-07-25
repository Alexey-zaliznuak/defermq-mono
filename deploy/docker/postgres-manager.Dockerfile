# syntax=docker/dockerfile:1.7
ARG GO_VERSION=1.26.0
FROM golang:${GO_VERSION}-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false \
    -ldflags="-s -w -X github.com/defermq/defermq/internal/buildinfo.Version=${VERSION} -X github.com/defermq/defermq/internal/buildinfo.Commit=${COMMIT} -X github.com/defermq/defermq/internal/buildinfo.BuildTime=${BUILD_TIME}" \
    -o /out/defermq-postgres-manager ./cmd/defermq-postgres-manager

FROM alpine:3.23
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S -g 10001 defermq \
    && adduser -S -D -H -u 10001 -G defermq defermq
COPY --from=builder --chown=defermq:defermq /out/defermq-postgres-manager /usr/local/bin/defermq-postgres-manager
USER defermq:defermq
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/defermq-postgres-manager"]
