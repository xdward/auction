# syntax=docker/dockerfile:1

#### Build Stage ####

FROM golang:1.26 AS build-stage

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

WORKDIR /app

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags "-s -w" -o expire -v ./cmd/worker/expire
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags "-s -w" -o sell -v ./cmd/worker/sell
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags "-s -w" -o bid -v ./cmd/worker/bid
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags "-s -w" -o cancel -v ./cmd/worker/cancel
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags "-s -w" -o server -v ./cmd/server

#### Release Stage ####

FROM gcr.io/distroless/static-debian12:nonroot-amd64 AS expire-worker
WORKDIR /
COPY --from=build-stage /app/expire /expire
ENTRYPOINT ["/expire"]

FROM gcr.io/distroless/static-debian12:nonroot-amd64 AS sell-worker
WORKDIR /
COPY --from=build-stage /app/sell /sell
ENTRYPOINT ["/sell"]

FROM gcr.io/distroless/static-debian12:nonroot-amd64 AS bid-worker
WORKDIR /
COPY --from=build-stage /app/bid /bid
ENTRYPOINT ["/bid"]

FROM gcr.io/distroless/static-debian12:nonroot-amd64 AS cancel-worker
WORKDIR /
COPY --from=build-stage /app/cancel /cancel
ENTRYPOINT ["/cancel"]

FROM gcr.io/distroless/static-debian12:nonroot-amd64 AS server
WORKDIR /
COPY --from=build-stage /app/server /server
ENTRYPOINT ["/server"]
