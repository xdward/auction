package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/nats-io/nats.go"
	pb "github.com/xdward/auction-contracts/gen/go"
	"github.com/xdward/auction/internal/server"
	"google.golang.org/grpc"
)

func main() {
	// help message
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s\n", os.Args[0])
		flag.PrintDefaults()
	}

	// parse flags
	port := flag.Int("port", 50051, "grpc server port")
	natsAddress := flag.String("address", nats.DefaultURL, "NATS server address")
	flag.Parse()

	// deny extra arguments
	if len(flag.Args()) != 0 {
		fmt.Fprintf(os.Stderr, "too many arguments: %s", flag.Args())
		os.Exit(1)
	}

	// create network socket
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		panic(err)
	}

	// create server
	s := grpc.NewServer()

	// open nats connection
	nc, err := nats.Connect(*natsAddress)
	if err != nil {
		panic(err)
	}
	defer nc.Drain()

	// link server resources
	pb.RegisterAuctionServiceServer(s, &server.Server{
		NATS: nc,
	})

	// start server
	slog.SetLogLoggerLevel(slog.LevelDebug)
	slog.Info(fmt.Sprintf("server listening at %s", lis.Addr()))
	if err := s.Serve(lis); err != nil {
		panic(err)
	}
}
