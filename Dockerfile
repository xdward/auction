# syntax=docker/dockerfile:1

#### Build Stage ####

FROM golang:1.26 AS build-stage

ENV CGO_ENABLED=0 \
    GOOS=linux

WORKDIR /app

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -o worker -v ./cmd/worker
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -o server -v ./cmd/server

#### Release Stage ####

FROM gcr.io/distroless/static-debian12:nonroot-amd64 AS worker
WORKDIR /
COPY --from=build-stage /app/worker /worker
ENTRYPOINT ["/worker"]

FROM gcr.io/distroless/static-debian12:nonroot-amd64 AS server
WORKDIR /
COPY --from=build-stage /app/server /server
ENTRYPOINT ["/server"]
