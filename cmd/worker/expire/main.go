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

	w := service.Worker{}

	service.NewScheduleConsumer(&w, natsAddress, "expire", w.HandleExpire)
}
