package server

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	pb "github.com/xdward/auction-contracts/gen/go"
	"github.com/xdward/auction/internal/service"
	"github.com/xdward/auction/internal/service/db"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Sell forwards a sell request to NATS and returns the response.
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

// Bid forwards a bid request to NATS and returns the response.
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

// Cancel forwards a cancel request to NATS and returns the response.
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

// EventStream streams the current snapshot and subsequent auction events.
func (s *Server) EventStream(_ *emptypb.Empty, stream pb.AuctionService_EventStreamServer) error {
	ctx := stream.Context()

	// send an initial snapshot and capture the current version
	snapshot, err := db.GetSnapshot(ctx, s.Redis)
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
		return status.Error(codes.Internal, "failed to send stream response message")
	}

	// using the version counter, resolve the id to start reading stream entries from
	cursor, err := s.Redis.Get(ctx, db.VersionToEntryPrefix+snapshot.Version).Result()
	if err == redis.Nil {
		cursor = "0-0"
	} else if err != nil {
		return status.Error(codes.Internal, "failed to resolve start stream id")
	}

	// stream unti the client disconnects/error
	for {
		// gracefully stop
		if err := ctx.Err(); err != nil {
			return status.Error(codes.Canceled, "closed stream")
		}

		// read a batch of stream entries, starting at the cursor location
		streams, err := s.Redis.XRead(ctx, &redis.XReadArgs{
			Streams: []string{db.StreamKey, cursor},
			Count:   10,
			Block:   0, // if there are no updates, block
		}).Result()
		if err != nil {
			slog.Error(err.Error())
			return status.Error(codes.Internal, "failed to read events")
		}

		// no new messages; loop
		if len(streams) == 0 {
			continue
		}

		// iterate each message in all streams (one stream)
		for _, st := range streams {
			for _, msg := range st.Messages {
				// read serialized event message
				rawData, err := service.ToBytes(msg.Values["data"])
				if err != nil {
					return service.TypeCastingErr
				}

				// create client response
				var response pb.EventStreamResponse

				// deserialize the event message into the correct type
				switch msg.Values["event"] {
				case db.SellEvent:
					var sellEvent pb.SellEvent
					if err := proto.Unmarshal(rawData, &sellEvent); err != nil {
						return err
					}
					response.Event = &pb.EventStreamResponse_SellEvent{SellEvent: &sellEvent}
				case db.BidEvent:
					var bidEvent pb.BidEvent
					if err := proto.Unmarshal(rawData, &bidEvent); err != nil {
						return err
					}
					response.Event = &pb.EventStreamResponse_BidEvent{BidEvent: &bidEvent}
				case db.CancelEvent:
					var cancelEvent pb.CancelEvent
					if err := proto.Unmarshal(rawData, &cancelEvent); err != nil {
						return err
					}
					response.Event = &pb.EventStreamResponse_CancelEvent{CancelEvent: &cancelEvent}
				case db.ExpireEvent:
					var expireEvent pb.ExpireEvent
					if err := proto.Unmarshal(rawData, &expireEvent); err != nil {
						return err
					}
					response.Event = &pb.EventStreamResponse_ExpireEvent{ExpireEvent: &expireEvent}
				default:
					return status.Error(codes.Internal, "invalid event type")
				}

				// send the message to the client
				if err := stream.Send(&response); err != nil {
					return status.Error(codes.Internal, "failed to handle message")
				}

				// advance cursor
				cursor = msg.ID
			}
		}
	}
}
