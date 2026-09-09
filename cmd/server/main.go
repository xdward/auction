package main

import (
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	pb "github.com/xdward/auction-contracts/gen/go"
	"github.com/xdward/auction/internal/auctionstore"
	"github.com/xdward/auction/internal/server"
	"google.golang.org/grpc"
)

var (
	deployment   = os.Getenv("STAGE")
	natsAddress  = os.Getenv("NATS_ADDRESS")
	redisAddress = os.Getenv("REDIS_ADDRESS")
)

func main() {
	if !(deployment == "prod" || deployment == "stage") {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	store := auctionstore.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "",
		DB:       0,
	})
	defer store.Close()

	nc, err := nats.Connect(natsAddress)
	if err != nil {
		panic(err)
	}

	s := grpc.NewServer(
		grpc.UnaryInterceptor(server.UnaryLoggingInterceptor),
		grpc.StreamInterceptor(server.StreamLoggingInterceptor),
	)
	pb.RegisterAuctionServiceServer(s, &server.Server{
		NATS:         nc,
		AuctionStore: store,
	})

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		panic(err)
	}

	slog.Info(fmt.Sprintf("server listening at %s", lis.Addr()))
	if err := s.Serve(lis); err != nil {
		panic(err)
	}
}
