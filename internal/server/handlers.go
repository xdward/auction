package server

import (
	"context"
	"log/slog"
	"time"

	pb "github.com/xdward/auction-contracts/gen/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Handler for the Sell method.
func (s *Server) Sell(ctx context.Context, req *pb.SellRequest) (*pb.SellResponse, error) {
	// serialize the protobuf request
	payload, err := proto.Marshal(req)
	if err != nil {
		slog.Error(err.Error())
		return nil, status.Error(codes.InvalidArgument, "malformed request")
	}

	// forward the request to NATS
	responseMsg, err := s.NATS.Request("event.sell", payload, time.Second)
	if err != nil {
		slog.Error(err.Error())
		return nil, status.Error(codes.Unavailable, "service unavailable")
	}

	// deserialize the NATS reply into the protobuf response and return it
	var res pb.SellResponse
	if err := proto.Unmarshal(responseMsg.Data, &res); err != nil {
		if len(responseMsg.Data) > 0 {
			slog.Error("worker: " + string(responseMsg.Data))
		} else {
			slog.Error(err.Error())
		}
		return nil, status.Error(codes.Internal, "failed to handle message")
	}

	return &res, nil
}

// Handler for the Bid method.
func (s *Server) Bid(ctx context.Context, req *pb.BidRequest) (*pb.BidResponse, error) {
	// serialize the protobuf request
	payload, err := proto.Marshal(req)
	if err != nil {
		slog.Error(err.Error())
		return nil, status.Error(codes.InvalidArgument, "malformed request")
	}

	// forward the request to NATS
	responseMsg, err := s.NATS.Request("event.bid", payload, time.Second)
	if err != nil {
		slog.Error(err.Error())
		return nil, status.Error(codes.Unavailable, "service unavailable")
	}

	// deserialize the NATS reply into the protobuf response and return it
	var res pb.BidResponse
	if err := proto.Unmarshal(responseMsg.Data, &res); err != nil {
		if len(responseMsg.Data) > 0 {
			slog.Error("worker: " + string(responseMsg.Data))
		} else {
			slog.Error(err.Error())
		}
		return nil, status.Error(codes.Internal, "failed to handle message")
	}

	return &res, nil
}

// Handler for the Cancel method.
func (s *Server) Cancel(ctx context.Context, req *pb.CancelRequest) (*pb.CancelResponse, error) {
	// serialize the protobuf request
	payload, err := proto.Marshal(req)
	if err != nil {
		slog.Error(err.Error())
		return nil, status.Error(codes.InvalidArgument, "malformed request")
	}

	// forward the request to NATS
	responseMsg, err := s.NATS.Request("event.cancel", payload, time.Second)
	if err != nil {
		slog.Error(err.Error())
		return nil, status.Error(codes.Unavailable, "service unavailable")
	}

	// deserialize the NATS reply into the protobuf response and return it
	var res pb.CancelResponse
	if err := proto.Unmarshal(responseMsg.Data, &res); err != nil {
		if len(responseMsg.Data) > 0 {
			slog.Error("worker: " + string(responseMsg.Data))
		} else {
			slog.Error(err.Error())
		}
		return nil, status.Error(codes.Internal, "failed to handle message")
	}

	return &res, nil
}

func (s *Server) EventStream(_ *emptypb.Empty, stream pb.AuctionService_EventStreamServer) error {
	return nil
}
