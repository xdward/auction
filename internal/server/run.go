package server

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/xdward/auction-contracts/pb/service"
	"google.golang.org/grpc"
)

func StartGRPCServer(lis net.Listener) {
	s := grpc.NewServer()
	service.RegisterAuctionServiceServer(s, &Server{})

	slog.Info("server listening at " + fmt.Sprintf("%s", lis.Addr()))
	if err := s.Serve(lis); err != nil {
		slog.Error("encountered an error", "err", err)
		panic(err)
	}
}
