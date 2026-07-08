package server

import (
	"context"
	"fmt"
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
	return serviceRPC[*pb.SellRequest, *pb.SellResponse](ctx, req, s, "sell")
}

// Handler for the Bid method.
func (s *Server) Bid(ctx context.Context, req *pb.BidRequest) (*pb.BidResponse, error) {
	return serviceRPC[*pb.BidRequest, *pb.BidResponse](ctx, req, s, "bid")
}

// Handler for the Cancel method.
func (s *Server) Cancel(ctx context.Context, req *pb.CancelRequest) (*pb.CancelResponse, error) {
	return serviceRPC[*pb.CancelRequest, *pb.CancelResponse](ctx, req, s, "cancel")
}

// Forwards an RPC request to NATS, then returns the corresponding response.
func serviceRPC[Request proto.Message, Response proto.Message](
	_ context.Context,
	req Request,
	s *Server,
	method string,
) (Response, error) {
	// serialize the protobuf request
	payload, err := proto.Marshal(req)
	if err != nil {
		slog.Error(err.Error())
		var zero Response
		return zero, status.Error(codes.InvalidArgument, "malformed request")
	}

	// forward the request to NATS
	subject := fmt.Sprintf("event.%s", method)
	responseMsg, err := s.NATS.Request(subject, payload, time.Second)
	if err != nil {
		slog.Error(err.Error())
		var zero Response
		return zero, status.Error(codes.Unavailable, "service unavailable")
	}

	// deserialize the NATS reply into the protobuf response and return it
	var res Response
	if err := proto.Unmarshal(responseMsg.Data, res); err != nil {
		if len(responseMsg.Data) > 0 {
			slog.Error("worker: " + string(responseMsg.Data))
		} else {
			slog.Error(err.Error())
		}
		var zero Response
		return zero, status.Error(codes.Internal, "failed to handle message")
	}

	return res, nil
}
