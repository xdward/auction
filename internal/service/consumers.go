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
func NewQueueSubscriber(w *Worker, subj string, queue string, cb nats.MsgHandler) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	_, err := w.NATS.QueueSubscribe(subj, queue, cb)
	if err != nil {
		panic(err)
	}

	slog.Info("watching", "queue", queue, "subject", subj)
	<-ctx.Done()
	slog.Info("shutting down")
}

// NewScheduleConsumer attaches a new consumer to a stream. The first invocation of this function
// creates a new stream with the AllowMsgSchedules flag enabled. A new durable pull consumer is also
// created, which the worker uses to handle messages. To schedule a message for future delivery,
// use the scheduling and target subjects shown in the example below:
//
//	msg := nats.NewMsg("subject.schedule.1")
//	msg.Data = []byte("hello")
//	msg.Header.Set("Nats-Schedule", "@at "+"2026-05-27T22:47:00Z")
//	msg.Header.Set("Nats-Schedule-Target", "auction.target.1")
//	ack, err := js.PublishMsg(ctx, msg)
//
// Multiple invocations of this function create a pool of workers that handle scheduled messages for
// the given subject.
func NewScheduleConsumer(w *Worker, subj string, handler jetstream.MessageHandler) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	streamConfig, targetSubject := ScheduleStreamConfig()

	stream, err := w.JS.Stream(ctx, streamConfig.Name)
	if err == jetstream.ErrStreamNotFound {
		slog.Warn("creating new stream", "config", streamConfig)
		stream, err = w.JS.CreateStream(ctx, streamConfig)
		if err != nil {
			panic(err)
		}
	} else if err != nil {
		panic(err)
	}

	consumerConfig := jetstream.ConsumerConfig{
		Durable:       subj + "-watcher",
		FilterSubject: targetSubject,
	}

	consumer, err := stream.CreateConsumer(ctx, consumerConfig)
	if err != nil {
		panic(err)
	}

	cc, err := consumer.Consume(handler)
	if err != nil {
		panic(err)
	}
	defer cc.Stop()

	slog.Info("watching", "stream", streamConfig.Name, "subject", targetSubject)
	<-ctx.Done()
	slog.Info("shutting down")
}
