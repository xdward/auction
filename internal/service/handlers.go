package service

import (
	"log/slog"

	"github.com/nats-io/nats.go"
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
