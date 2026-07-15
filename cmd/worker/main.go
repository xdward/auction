package main

import (
	"flag"
	"os"

	"github.com/nats-io/nats.go"
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

	w := service.Worker{}

	switch *task {
	case "sell":
		service.NewQueueSubscriber(
			&w,
			natsAddress,
			redisAddress,
			"event.sell",
			"sell-workers",
			w.HandleSell,
		)
	case "bid":
		service.NewQueueSubscriber(
			&w,
			natsAddress,
			redisAddress,
			"event.bid",
			"bid-workers",
			w.HandleBid,
		)
	case "cancel":
		service.NewQueueSubscriber(
			&w,
			natsAddress,
			redisAddress,
			"event.cancel",
			"cancel-workers",
			w.HandleCancel,
		)
	case "expire":
		service.NewScheduleConsumer(
			&w,
			natsAddress,
			redisAddress,
			"expire",
			w.HandleExpire,
		)
	default:
		panic("invalid --task (must be: sell, bid, cancel, expire)")
	}
}
