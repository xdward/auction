package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/go-redis/v9"
	"github.com/xdward/auction/internal/auctionstore"
	"github.com/xdward/auction/internal/messaging"
)

var (
	deployment   = os.Getenv("STAGE")
	natsAddress  = os.Getenv("NATS_ADDRESS")
	redisAddress = os.Getenv("REDIS_ADDRESS")

	task = flag.String("task", "", "event to handle: sell, bid, cancel, expire")
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	flag.Parse()

	if !(deployment == "prod" || deployment == "stage") {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	store := auctionstore.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "",
		DB:       0,
	})
	defer store.Close()

	nc, err := nats.Connect(natsAddress)
	if err != nil {
		panic(err)
	}
	defer nc.Drain()

	switch *task {
	case "sell":
		stream, err := jetstream.New(nc)
		if err != nil {
			panic(err)
		}
		_, err = nc.QueueSubscribe("event.sell", "sell.workers", messaging.SellHandler(store, stream))
		if err != nil {
			panic(err)
		}
		<-ctx.Done()
	case "bid":
		_, err := nc.QueueSubscribe("event.bid", "bid.workers", messaging.BidHandler(store))
		if err != nil {
			panic(err)
		}
		<-ctx.Done()
	case "cancel":
		_, err := nc.QueueSubscribe("event.cancel", "cancel.workers", messaging.CancelHandler(store))
		if err != nil {
			panic(err)
		}
		<-ctx.Done()
	case "expire":
		js, err := jetstream.New(nc)
		if err != nil {
			panic(err)
		}

		stream, err := js.Stream(ctx, messaging.SCHEDULE_STREAM)
		if err != nil {
			panic(err)
		}

		consumer, err := stream.Consumer(ctx, messaging.SCHEDULE_CONSUMER)
		if err != nil {
			panic(err)
		}

		cc, err := consumer.Consume(messaging.ExpireHandler(store))
		if err != nil {
			panic(err)
		}

		defer cc.Stop()
		<-ctx.Done()
	default:
		panic("invalid --task (must be: sell, bid, cancel, expire)")
	}
}
