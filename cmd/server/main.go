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

	nc, err := nats.Connect(natsAddress, nats.Token(os.Getenv("NATS_TOKEN")))
	if err != nil {
		panic(err)
	}
	defer nc.Drain()

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: os.Getenv("REDIS_PASS"),
		DB:       0,
	})
	defer rdb.Close()

	s := grpc.NewServer()
	pb.RegisterAuctionServiceServer(s, &server.Server{
		NATS:  nc,
		Redis: rdb,
	})

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		panic(err)
	}

	slog.SetLogLoggerLevel(slog.LevelDebug)
	slog.Info(fmt.Sprintf("server listening at %s", lis.Addr()))
	if err := s.Serve(lis); err != nil {
		panic(err)
	}
}
