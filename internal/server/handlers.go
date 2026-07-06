package server

import (
	"context"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	pb "github.com/xdward/auction-contracts/gen/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Implementation of the AuctionService server.
type Server struct {
	// Embedded for forward compatability.
	pb.UnimplementedAuctionServiceServer

	// Shared NATS connection.
	NATS *nats.Conn
}

// Handler for the Sell method.
func (s *Server) Sell(ctx context.Context, req *pb.SellRequest) (*pb.SellResponse, error) {
	slog.Debug("received message", "method", "sell")

	rawRequest, err := proto.Marshal(req)
	if err != nil {
		slog.Error("failed to encode protobuf message")
		return nil, status.Error(codes.Internal, "failed to handle message")
	}

	res, err := s.NATS.Request("event.sell", rawRequest, time.Second)
	if err != nil {
		slog.Error("failed to send NATS request", "err", err)
		return nil, status.Error(codes.Internal, "failed to handle message")
	}

	var response pb.SellResponse
	if err := proto.Unmarshal(res.Data, &response); err != nil {
		slog.Error("failed to parse raw data into protobuf message")
		return nil, status.Error(codes.Internal, "failed to handle message")
	}

	return &response, nil
}

// Handler for the Bid method.
func (s *Server) Bid(ctx context.Context, req *pb.BidRequest) (*pb.BidResponse, error) {
	slog.Debug("received message", "method", "bid")

	rawRequest, err := proto.Marshal(req)
	if err != nil {
		slog.Error("failed to encode protobuf message")
		return nil, status.Error(codes.Internal, "failed to handle message")
	}

	res, err := s.NATS.Request("event.bid", rawRequest, time.Second)
	if err != nil {
		slog.Error("failed to send NATS request", "err", err)
		return nil, status.Error(codes.Internal, "failed to handle message")
	}

	var response pb.BidResponse
	if err := proto.Unmarshal(res.Data, &response); err != nil {
		slog.Error("failed to parse raw data into protobuf message")
		return nil, status.Error(codes.Internal, "failed to handle message")
	}

	return &response, nil
}

// Handler for the Cancel method.
func (s *Server) Cancel(ctx context.Context, req *pb.CancelRequest) (*pb.CancelResponse, error) {
	slog.Debug("received message", "method", "cancel")

	rawRequest, err := proto.Marshal(req)
	if err != nil {
		slog.Error("failed to encode protobuf message")
		return nil, status.Error(codes.Internal, "failed to handle message")
	}

	res, err := s.NATS.Request("event.cancel", rawRequest, time.Second)
	if err != nil {
		slog.Error("failed to send NATS request", "err", err)
		return nil, status.Error(codes.Internal, "failed to handle message")
	}

	var response pb.CancelResponse
	if err := proto.Unmarshal(res.Data, &response); err != nil {
		slog.Error("failed to parse raw data into protobuf message")
		return nil, status.Error(codes.Internal, "failed to handle message")
	}

	return &response, nil
}
