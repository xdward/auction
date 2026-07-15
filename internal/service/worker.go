package service

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/xdward/auction/internal/service/db"
)

type Worker struct {
	NATS *nats.Conn
	JS   jetstream.JetStream
	DB   *db.Client
}
