package messaging

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// RunQueueSubscriber registers a queue subscriber and blocks until the context is done.
func RunQueueSubscriber(
	ctx context.Context,
	nc *nats.Conn,
	subj string,
	queue string,
	cb nats.MsgHandler,
) error {
	_, err := nc.QueueSubscribe(subj, queue, cb)
	if err != nil {
		return err
	}

	<-ctx.Done()
	return ctx.Err()
}

// RunScheduleConsumer attaches a durable consumer to the configured stream and blocks until the
// context is done.
func RunScheduleConsumer(
	ctx context.Context,
	js jetstream.JetStream,
	subj string,
	handler jetstream.MessageHandler,
) error {
	streamConfig, targetSubject := ScheduleStreamConfig()

	stream, err := js.Stream(ctx, streamConfig.Name)
	if err == jetstream.ErrStreamNotFound {
		stream, err = js.CreateStream(ctx, streamConfig)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	consumerConfig := jetstream.ConsumerConfig{
		Durable:       subj + "-watcher",
		FilterSubject: targetSubject,
	}

	consumer, err := stream.CreateConsumer(ctx, consumerConfig)
	if err != nil {
		return err
	}

	cc, err := consumer.Consume(handler)
	if err != nil {
		return err
	}
	defer cc.Stop()

	<-ctx.Done()
	return ctx.Err()
}
