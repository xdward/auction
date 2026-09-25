# syntax=docker/dockerfile:1

#### Setup Stage ####

FROM golang:1.27 AS base-stage

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

WORKDIR /app

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

#### Worker Build Stage ####

FROM base-stage AS worker-build
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags "-s -w" -trimpath -buildvcs=false -o worker ./cmd/worker

#### Server Build Stage ####

FROM base-stage AS server-build
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags "-s -w" -trimpath -buildvcs=false -o server ./cmd/server

#### Release Stage ####

FROM gcr.io/distroless/static-debian12:nonroot-amd64 AS worker
WORKDIR /
COPY --from=worker-build /app/worker /worker
ENTRYPOINT ["/worker"]

FROM gcr.io/distroless/static-debian12:nonroot-amd64 AS server
WORKDIR /
COPY --from=server-build /app/server /server
ENTRYPOINT ["/server"]
