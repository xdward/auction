package service

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Worker struct {
	NATS *nats.Conn
	JS   jetstream.JetStream
}
