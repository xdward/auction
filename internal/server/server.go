package server

import (
	"github.com/nats-io/nats.go"
	pb "github.com/xdward/auction-contracts/gen/go"
)

// Implementation of the AuctionService server.
type Server struct {
	// Embedded for forward compatability.
	pb.UnimplementedAuctionServiceServer

	// Shared NATS connection.
	NATS *nats.Conn
}
