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
	natsToken     = os.Getenv("NATS_TOKEN")
	redisPassword = os.Getenv("REDIS_PASS")
	deployment    = os.Getenv("STAGE")
)

func main() {
	if !(deployment == "prod" || deployment == "stage") {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

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

	store := auctionstore.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: redisPassword,
		DB:       0,
	})
	defer store.Close()

	nc, err := nats.Connect(natsAddress, nats.Token(natsToken))
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

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		panic(err)
	}

	slog.Info(fmt.Sprintf("server listening at %s", lis.Addr()))
	if err := s.Serve(lis); err != nil {
		panic(err)
	}
}
