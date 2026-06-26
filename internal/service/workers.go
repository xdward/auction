package service

import (
	"context"
	"log/slog"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
)

// StartQueueWorker registers a new subscriber to a queue. A single queue subscriber will receive
// messages for the subject it is subscribed to. Multiple queue subscribers with the same subject
// and queue form a queue group. Messages sent to the queue group’s subject are delivered to exactly
// one subscriber, which is randomly chosen within the group.
func StartQueueWorker(subject string, queue string, handler nats.MsgHandler) error {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		slog.Error("connection failed")
		return err
	}
	defer nc.Drain()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	_, err = nc.QueueSubscribe(subject, queue, handler)
	if err != nil {
		slog.Error("subscription failed", "queue", queue, "subject", subject)
		return err
	}

	slog.Info("connected", "queue", queue, "subject", subject)
	<-ctx.Done()

	slog.Info("shutting down")
	return nil
}
