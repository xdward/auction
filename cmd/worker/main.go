package main

import (
	"flag"
	"os"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/go-redis/v9"
	"github.com/xdward/auction/internal/service"
	"github.com/xdward/auction/internal/service/db"
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

	nc, err := nats.Connect(natsAddress)
	if err != nil {
		panic(err)
	}
	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		panic(err)
	}

	rdb := db.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "",
		DB:       0,
	})
	defer rdb.Close()

	w := service.Worker{
		NATS: nc,
		JS:   js,
		DB:   rdb,
	}

	switch *task {
	case "sell":
		service.NewQueueSubscriber(&w, "event.sell", "sell-workers", w.HandleSell)
	case "bid":
		service.NewQueueSubscriber(&w, "event.bid", "bid-workers", w.HandleBid)
	case "cancel":
		service.NewQueueSubscriber(&w, "event.cancel", "cancel-workers", w.HandleCancel)
	case "expire":
		service.NewScheduleConsumer(&w, "expire", w.HandleExpire)
	default:
		panic("invalid --task (must be: sell, bid, cancel, expire)")
	}
}
