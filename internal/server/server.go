package server

import (
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	pb "github.com/xdward/auction-contracts/gen/go"
)

// Implementation of the AuctionService server.
type Server struct {
	// Embedded for forward compatability.
	pb.UnimplementedAuctionServiceServer

	NATS  *nats.Conn    // Shared NATS connection.
	Redis *redis.Client // Shared Redis client.
}
