package service

import (
	"context"
	"log/slog"
	"os/signal"
	"strings"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// StartQueueWorker registers a new subscriber to a queue. A single queue subscriber will receive
// messages for the subject it is subscribed to. Multiple queue subscribers with the same subject
// and queue form a queue group. Messages sent to the queue group’s subject are delivered to exactly
// one subscriber, which is randomly chosen within the group.
func StartQueueWorker(wr *WorkerResources, subj string, queue string, cb nats.MsgHandler) error {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	_, err := wr.NATS.QueueSubscribe(subj, queue, cb)
	if err != nil {
		slog.Error("subscription failed", "queue", queue, "subject", subj)
		return err
	}

	slog.Info("watching", "queue", queue, "subject", subj)
	<-ctx.Done()

	slog.Info("shutting down")
	return nil
}

// StartScheduleWorker attaches a new consumer to a stream. The first invocation of this function
// creates a new stream with the AllowMsgSchedules flag enabled. A new durable pull consumer is also
// created, which the worker uses to handle messages. To schedule a message for future delivery,
// use the scheduling and target subjects shown in the example below:
//
//	msg := nats.NewMsg("subject.schedule.1")
//	msg.Data = []byte("hello")
//	msg.Header.Set("Nats-Schedule", "@at "+"2026-05-27T22:47:00Z")
//	msg.Header.Set("Nats-Schedule-Target", "expiration.target.1")
//	ack, err := js.PublishMsg(ctx, msg)
//
// Multiple invocations of this function create a pool of workers that handle scheduled messages for
// the given subject.
func StartScheduleWorker(wr *WorkerResources, subj string, handler jetstream.MessageHandler) error {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	scheduleSubject := subj + ".schedule.*"
	targetSubject := subj + ".target.*"

	streamConfig := jetstream.StreamConfig{
		Name:              strings.ToUpper(subj) + "_SCHEDULES",
		Subjects:          []string{scheduleSubject, targetSubject},
		AllowMsgSchedules: true, // cannot be disabled
	}

	stream, err := wr.JS.Stream(ctx, streamConfig.Name)
	if err == jetstream.ErrStreamNotFound {
		slog.Warn("creating new stream", "config", streamConfig)
		stream, err = wr.JS.CreateStream(ctx, streamConfig)
		if err != nil {
			slog.Error("failed to create stream")
			return err
		}
	} else if err != nil {
		slog.Error("failed to get stream interface", "name", streamConfig.Name)
		return err
	}

	consumerConfig := jetstream.ConsumerConfig{
		Durable:       subj + "-watcher",
		FilterSubject: targetSubject,
	}

	consumer, err := stream.CreateConsumer(ctx, consumerConfig)
	if err != nil {
		slog.Error("failed to create consumer", "stream", streamConfig, "config", consumerConfig)
		return err
	}

	cc, err := consumer.Consume(handler)
	if err != nil {
		slog.Error(
			"failed to start consumer",
			"stream", streamConfig,
			"config", consumerConfig,
			"info", consumer.CachedInfo(),
		)
		return err
	}
	defer cc.Stop()

	slog.Info("watching", "stream", streamConfig.Name, "subject", targetSubject)

	<-ctx.Done()

	slog.Info("shutting down")
	return nil
}
