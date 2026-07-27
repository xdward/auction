package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/go-redis/v9"
	"github.com/xdward/auction/internal/auctionstore"
	"github.com/xdward/auction/internal/messaging"
	"github.com/xdward/auction/internal/service"
)

func main() {
	natsAddress, ok := os.LookupEnv("NATS_ADDRESS")
	if !ok {
		natsAddress = nats.DefaultURL
	}
	redisAddress, ok := os.LookupEnv("REDIS_ADDRESS")
	if !ok {
		redisAddress = "localhost:6379"
	}

	task := flag.String("task", "", "event to handle: sell, bid, cancel, expire")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	nc, err := nats.Connect(natsAddress)
	if err != nil {
		panic(err)
	}
	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		panic(err)
	}

	store := auctionstore.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "",
		DB:       0,
	})
	defer store.Close()

	w := service.Worker{
		NATS:         nc,
		JS:           js,
		AuctionStore: store,
	}

	switch *task {
	case "sell":
		err = messaging.RunQueueSubscriber(ctx, nc, "event.sell", "sell-workers", w.HandleSell)
		if err != nil && err != context.Canceled {
			panic(err)
		}
	case "bid":
		err = messaging.RunQueueSubscriber(ctx, nc, "event.bid", "bid-workers", w.HandleBid)
		if err != nil && err != context.Canceled {
			panic(err)
		}
	case "cancel":
		err = messaging.RunQueueSubscriber(ctx, nc, "event.cancel", "cancel-workers", w.HandleCancel)
		if err != nil && err != context.Canceled {
			panic(err)
		}
	case "expire":
		err = messaging.RunScheduleConsumer(ctx, js, "expire", w.HandleExpire)
		if err != nil && err != context.Canceled {
			panic(err)
		}
	default:
		panic("invalid --task (must be: sell, bid, cancel, expire)")
	}
}
