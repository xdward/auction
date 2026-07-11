package main

import (
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

	w := service.Worker{}

	service.NewScheduleConsumer(&w, natsAddress, redisAddress, "expire", w.HandleExpire)
}
