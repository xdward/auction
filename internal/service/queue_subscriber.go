package service

import (
	"context"
	"log/slog"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// NewQueueSubscriber registers a new subscriber to a queue. A single queue subscriber will
// receive messages for the subject it is subscribed to. Multiple queue subscribers with the same
// subject and queue form a queue group. Messages sent to the queue group’s subject are delivered to
// exactly one subscriber, which is randomly chosen within the group.
func NewQueueSubscriber(w *Worker, addr string, subj string, queue string, cb nats.MsgHandler) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	nc, err := nats.Connect(addr)
	if err != nil {
		panic(err)
	}
	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		panic(err)
	}

	w.NATS = nc
	w.JS = js

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	_, err = w.NATS.QueueSubscribe(subj, queue, cb)
	if err != nil {
		panic(err)
	}

	slog.Info("watching", "queue", queue, "subject", subj)
	<-ctx.Done()
	slog.Info("shutting down")
}
