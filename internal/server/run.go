package server

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/nats-io/nats.go"
	pb "github.com/xdward/auction-contracts/gen/go"
	"google.golang.org/grpc"
)

// Starts a gRPC server for AuctionService. Requests are dispatched to the service implementation.
func StartAuctionServiceServer(lis net.Listener) {
	s := grpc.NewServer()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		slog.Error("failed to register NATS connection")
		panic(err)
	}
	defer nc.Drain()

	pb.RegisterAuctionServiceServer(s, &Server{
		NATS: nc,
	})

	slog.Info("server listening at " + fmt.Sprintf("%s", lis.Addr()))
	if err := s.Serve(lis); err != nil {
		slog.Error("encountered an error", "err", err)
		panic(err)
	}
}
