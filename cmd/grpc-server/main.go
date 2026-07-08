package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"

	server "github.com/xdward/auction/internal/server"
)

var (
	port = flag.Int("port", 50051, "the server port")
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	server.StartAuctionServiceServer(lis)
}
