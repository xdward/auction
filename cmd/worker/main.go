package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"github.com/xdward/auction/internal/auctionstore"
	"github.com/xdward/auction/internal/service"
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
		_, err = service.RegisterQueueSubscriber(
			nc,
			"event.sell",
			"sell.workers",
			service.SellHandler(store, nc),
		)
		if err != nil {
			panic(err)
		}
	case "bid":
		_, err := service.RegisterQueueSubscriber(
			nc,
			"event.bid",
			"bid.workers",
			service.BidHandler(store),
		)
		if err != nil {
			panic(err)
		}
	case "cancel":
		_, err := service.RegisterQueueSubscriber(nc,
			"event.cancel",
			"cancel.workers",
			service.CancelHandler(store),
		)
		if err != nil {
			panic(err)
		}
	case "expire":
		_, err := service.RegisterScheduleConsumer(ctx, nc, service.ExpireHandler(store))
		if err != nil {
			panic(err)
		}
	default:
		panic("invalid --task (must be: sell, bid, cancel, expire)")
	}

	<-ctx.Done()
}
