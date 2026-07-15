package main

import (
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	pb "github.com/xdward/auction-contracts/gen/go"
	"github.com/xdward/auction/internal/server"
	"google.golang.org/grpc"
)

func main() {
	// retrieve environment variables
	port, ok := os.LookupEnv("GRPC_SERVER_PORT")
	if !ok {
		port = "50051"
	}
	natsAddress, ok := os.LookupEnv("NATS_ADDRESS")
	if !ok {
		natsAddress = nats.DefaultURL
	}
	redisAddress, ok := os.LookupEnv("REDIS_ADDRESS")
	if !ok {
		redisAddress = "localhost:6379"
	}

	// create network socket
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		panic(err)
	}

	// create server
	s := grpc.NewServer()

	// open nats connection
	nc, err := nats.Connect(natsAddress)
	if err != nil {
		panic(err)
	}
	defer nc.Drain()

	// create redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "", // no password parameter
		DB:       0,  // use default db
	})
	defer rdb.Close()

	// register service
	pb.RegisterAuctionServiceServer(s, &server.Server{
		NATS:  nc,
		Redis: rdb,
	})

	// start server
	slog.SetLogLoggerLevel(slog.LevelDebug)
	slog.Info(fmt.Sprintf("server listening at %s", lis.Addr()))
	if err := s.Serve(lis); err != nil {
		panic(err)
	}
}
