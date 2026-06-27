package service

import (
	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TODO
func HandleBuy(msg *nats.Msg) {
	slog.Debug("received message", "data", msg.Data)
	msg.Respond([]byte("read message"))
}

// TODO
func HandleSell(msg *nats.Msg) {
	slog.Debug("received message", "data", msg.Data)
	msg.Respond([]byte("read message"))
}

// TODO
func HandleCancel(msg *nats.Msg) {
	slog.Debug("received message", "data", msg.Data)
	msg.Respond([]byte("read message"))
}

// TODO
func HandleExpiration(msg jetstream.Msg) {
	slog.Debug("received message", "data", msg.Data())
	msg.Ack()
}
