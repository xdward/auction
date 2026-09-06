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
    go build -ldflags "-s -w" -o worker -v ./cmd/worker
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags "-s -w" -o server -v ./cmd/server

#### Test Stage ####

FROM build-stage AS test-stage
RUN go test -v ./...

FROM test-stage AS artifacts
COPY --from=build-stage /app/worker /worker
COPY --from=build-stage /app/server /server

#### Release Stage ####

FROM gcr.io/distroless/static-debian12:nonroot-amd64 AS worker
WORKDIR /
COPY --from=artifacts /worker /worker
ENTRYPOINT ["/worker"]

FROM gcr.io/distroless/static-debian12:nonroot-amd64 AS server
WORKDIR /
COPY --from=artifacts /server /server
ENTRYPOINT ["/server"]
