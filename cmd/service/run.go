package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/xdward/auction/internal/service"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <buy|sell|cancel>\n", os.Args[0])
		os.Exit(1)
	}

	switch os.Args[1] {
	case "buy":
		err := service.StartQueueWorker("event.buy", "buy-workers", service.HandleBuy)
		if err != nil {
			panic(err)
		}
	case "sell":
		err := service.StartQueueWorker("event.sell", "sell-workers", service.HandleSell)
		if err != nil {
			panic(err)
		}
	case "cancel":
		err := service.StartQueueWorker("event.cancel", "cancel-workers", service.HandleBuy)
		if err != nil {
			panic(err)
		}
	case "expiration":
		err := service.StartScheduleWorker("expiration", service.HandleExpiration)
		if err != nil {
			panic(err)
		}
	default:
		fmt.Fprintf(os.Stderr, "invalid arg: %s\n", os.Args[1])
		os.Exit(1)
	}
}
