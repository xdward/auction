package service

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/go-redis/v9"
)

type Worker struct {
	NATS  *nats.Conn
	JS    jetstream.JetStream
	Redis *redis.Client
}
