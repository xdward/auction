package service

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// worker defines the lifecycle hooks shared by background service workers.
type worker interface {
}

// queueSubscriber wraps the active NATS queue subscription.
type queueSubscriber struct {
	// sub holds the underlying queue subscription.
	sub *nats.Subscription
}

// scheduleConsumer wraps the active JetStream consumer for scheduled jobs.
type scheduleConsumer struct {
	// con holds the underlying JetStream consumer.
	con *jetstream.Consumer
}

// RegisterQueueSubscriber subscribes the given callback to a NATS queue subject.
func RegisterQueueSubscriber(
	nc *nats.Conn,
	subj string,
	queue string,
	cb nats.MsgHandler,
) (*queueSubscriber, error) {
	sub, err := nc.QueueSubscribe(subj, queue, cb)
	if err != nil {
		return nil, err
	}

	return &queueSubscriber{
		sub: sub,
	}, nil
}

// RegisterScheduleConsumer creates and starts the schedule consumer for the configured stream.
func RegisterScheduleConsumer(
	ctx context.Context,
	nc *nats.Conn,
	handler jetstream.MessageHandler,
) (*scheduleConsumer, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	stream, err := js.Stream(ctx, SCHEDULE_STREAM)
	if err != nil {
		return nil, err
	}

	consumer, err := stream.Consumer(ctx, SCHEDULE_CONSUMER)
	if err != nil {
		return nil, err
	}

	_, err = consumer.Consume(handler)
	if err != nil {
		return nil, err
	}

	return &scheduleConsumer{
		con: &consumer,
	}, nil
}
