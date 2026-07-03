package server

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/nats-io/nats.go"
	"github.com/xdward/auction-contracts/pb/service"
	"google.golang.org/grpc"
)

func StartGRPCServer(lis net.Listener) {
	s := grpc.NewServer()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		slog.Error("failed to register NATS connection")
		panic(err)
	}
	defer nc.Drain()

	service.RegisterAuctionServiceServer(s, &Server{
		NATS: nc,
	})

	slog.Info("server listening at " + fmt.Sprintf("%s", lis.Addr()))
	if err := s.Serve(lis); err != nil {
		slog.Error("encountered an error", "err", err)
		panic(err)
	}
}
