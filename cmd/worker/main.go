package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/xdward/auction/internal/service"
)

func main() {
	// help message
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s\n", os.Args[0])
		flag.PrintDefaults()
	}

	// parse flags
	workerPtr := flag.String("worker", "", "buy | sell | cancel | expiration")
	addrPtr := flag.String("address", nats.DefaultURL, "NATS server address")
	flag.Parse()

	// deny extra arguments
	if len(flag.Args()) != 0 {
		fmt.Fprintf(os.Stderr, "too many arguments: %s", flag.Args())
		os.Exit(1)
	}

	// create worker resources
	nc, err := nats.Connect(*addrPtr)
	if err != nil {
		panic(err)
	}
	defer nc.Drain()

	// create jetstream instance
	js, err := jetstream.New(nc)
	if err != nil {
		panic(err)
	}

	// create worker resources
	wr := &service.WorkerResources{
		NATS: nc,
		JS:   js,
	}

	// start the chosen worker
	switch *workerPtr {
	case "buy":
		err := service.StartQueueWorker(wr, "event.buy", "buy-workers", wr.HandleBuy)
		if err != nil {
			panic(err)
		}
	case "sell":
		err := service.StartQueueWorker(wr, "event.sell", "sell-workers", wr.HandleSell)
		if err != nil {
			panic(err)
		}
	case "cancel":
		err := service.StartQueueWorker(wr, "event.cancel", "cancel-workers", wr.HandleBuy)
		if err != nil {
			panic(err)
		}
	case "expiration":
		err := service.StartScheduleWorker(wr, "expiration", wr.HandleExpiration)
		if err != nil {
			panic(err)
		}
	default:
		fmt.Fprintf(os.Stderr, "invalid flag argument: %s", *workerPtr)
		os.Exit(1)
	}
}
