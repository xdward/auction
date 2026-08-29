package server

import (
	"context"
	"log/slog"

	"github.com/nats-io/nats.go"
	pb "github.com/xdward/auction-contracts/gen/go"
	"github.com/xdward/auction/internal/auctionstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// AuctionServiceServer implements the AuctionService gRPC server.
type Server struct {
	// Embedded to satisfy forward compatibility with future RPC additions.
	pb.UnimplementedAuctionServiceServer

	NATS         *nats.Conn           // Shared NATS connection.
	AuctionStore *auctionstore.Client // Shared AuctionStore client.
}

// Sell validates a sell request and forwards it to the sell event handler.
func (s *Server) Sell(ctx context.Context, req *pb.SellRequest) (*pb.SellResponse, error) {
	if req.ItemId == 0 || req.SellerId == 0 || req.Duration == 0 {
		return nil, status.Error(codes.InvalidArgument, "all fields must be nonzero")
	}

	var res pb.SellResponse
	if err := natsRequestReply(s.NATS, "event.sell", req, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

// Bid validates a bid request and forwards it to the bid event handler.
func (s *Server) Bid(ctx context.Context, req *pb.BidRequest) (*pb.BidResponse, error) {
	if req.ItemId == 0 || req.BidderId == 0 || req.Amount == 0 {
		return nil, status.Error(codes.InvalidArgument, "all fields must be nonzero")
	}

	var res pb.BidResponse
	if err := natsRequestReply(s.NATS, "event.bid", req, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

// Cancel validates a cancel request and forwards it to the cancel event handler.
func (s *Server) Cancel(ctx context.Context, req *pb.CancelRequest) (*pb.CancelResponse, error) {
	if req.ItemId == 0 || req.SellerId == 0 {
		return nil, status.Error(codes.InvalidArgument, "all fields must be nonzero")
	}

	var res pb.CancelResponse
	if err := natsRequestReply(s.NATS, "event.cancel", req, &res); err != nil {
		return nil, err
	}

	return &res, nil
}

// EventStream streams the current snapshot and subsequent auction events.
func (s *Server) EventStream(_ *emptypb.Empty, stream pb.AuctionService_EventStreamServer) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	snapshot, cursorPtr, err := s.AuctionStore.GetSnapshot(ctx)
	if err != nil {
		slog.Error("failed to get snapshot",
			slog.String("error", err.Error()),
		)
		return status.Error(codes.Internal, "failed to get snapshot")
	}

	snapshotResposne := &pb.EventStreamResponse{
		Event: &pb.EventStreamResponse_Snapshot{
			Snapshot: snapshot,
		},
	}

	if err := stream.Send(snapshotResposne); err != nil {
		slog.Error("failed to send snapshot",
			slog.String("error", err.Error()),
			slog.Group("snapshot",
				slog.Int("size", len(snapshot.Listings)),
			),
		)
		return status.Error(codes.Internal, "failed to send snapshot")
	}

	for {
		events, newCursorPtr, err := s.AuctionStore.BatchReadStream(ctx, 10, *cursorPtr)
		if err != nil {
			slog.Error("failed to read event stream",
				slog.String("error", err.Error()),
				slog.String("cursor", *newCursorPtr),
			)
			return status.Error(codes.Internal, "failed to read event stream")
		}

		for _, e := range events {
			if err := stream.Send(e); err != nil {
				slog.Error("failed to send stream message",
					slog.String("error", err.Error()),
					slog.Any("message", e),
				)
				return status.Error(codes.Internal, "failed to send stream message")
			}
		}

		cursorPtr = newCursorPtr
	}
}
