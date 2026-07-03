package server

import (
	"context"

	msg "github.com/xdward/auction-contracts/pb/message"
	service "github.com/xdward/auction-contracts/pb/service"
)

type Server struct {
	service.UnimplementedAuctionServiceServer
}

func (s *Server) Sell(ctx context.Context, req *msg.SellRequest) (*msg.SellResponse, error) {
	// TODO
	return &msg.SellResponse{Success: true}, nil
}

func (s *Server) Bid(ctx context.Context, req *msg.BidRequest) (*msg.BidResponse, error) {
	// TODO
	return &msg.BidResponse{Success: true}, nil
}

func (s *Server) Cancel(ctx context.Context, req *msg.CancelRequest) (*msg.CancelResponse, error) {
	// TODO
	return &msg.CancelResponse{Success: true}, nil

}
