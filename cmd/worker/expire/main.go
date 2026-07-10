package main

import (
	"flag"

	"github.com/nats-io/nats.go"
	"github.com/xdward/auction/internal/service"
)

func main() {
	addrPtr := flag.String("address", nats.DefaultURL, "NATS server address")
	flag.Parse()

	w := service.Worker{}

	service.NewScheduleConsumer(&w, *addrPtr, "expire", w.HandleExpire)
}
