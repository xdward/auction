package service

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/xdward/auction/internal/auctionstore"
)

type Worker struct {
	NATS         *nats.Conn
	JS           jetstream.JetStream
	AuctionStore *auctionstore.Client
}
