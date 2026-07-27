package server

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	pb "github.com/xdward/auction-contracts/gen/go"
	"github.com/xdward/auction/internal/auctionstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Sell forwards a sell request to NATS and returns the response.
func (s *Server) Sell(ctx context.Context, req *pb.SellRequest) (*pb.SellResponse, error) {
	if req.ItemId == 0 || req.SellerId == 0 || req.Duration == 0 {
		return nil, status.Error(codes.InvalidArgument, "all fields must be nonzero")
	}

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

// Bid forwards a bid request to NATS and returns the response.
func (s *Server) Bid(ctx context.Context, req *pb.BidRequest) (*pb.BidResponse, error) {
	if req.ItemId == 0 || req.BidderId == 0 || req.Amount == 0 {
		return nil, status.Error(codes.InvalidArgument, "all fields must be nonzero")
	}

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

// Cancel forwards a cancel request to NATS and returns the response.
func (s *Server) Cancel(ctx context.Context, req *pb.CancelRequest) (*pb.CancelResponse, error) {
	if req.ItemId == 0 || req.SellerId == 0 {
		return nil, status.Error(codes.InvalidArgument, "all fields must be nonzero")
	}

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

// EventStream streams the current snapshot and subsequent auction events.
func (s *Server) EventStream(_ *emptypb.Empty, stream pb.AuctionService_EventStreamServer) error {
	ctx := stream.Context()

	// send an initial snapshot and capture the current version
	snapshot, err := auctionstore.GetSnapshot(ctx, s.Redis)
	if err != nil {
		slog.Error(err.Error())
		return status.Error(codes.Internal, "failed to get snapshot")
	}

	err = stream.Send(&pb.EventStreamResponse{
		Event: &pb.EventStreamResponse_Snapshot{
			Snapshot: snapshot,
		},
	})

	if err != nil {
		return status.Error(codes.Internal, "failed to send snapshot")
	}

	// resolve the stream cursor from the snapshot version
	cursor, err := s.Redis.Get(ctx, auctionstore.VersionToEntryPrefix+snapshot.Version).Result()
	if err == redis.Nil {
		cursor = "0-0"
	} else if err != nil {
		return status.Error(codes.Internal, "failed to get stream cursor")
	}

	return s.streamEvents(ctx, stream, cursor)
}
