package server

import (
	"context"
	"errors"
	"log/slog"

	"github.com/redis/go-redis/v9"
	pb "github.com/xdward/auction-contracts/gen/go"
	"github.com/xdward/auction/internal/service/db"
	"github.com/xdward/auction/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (s *Server) streamEvents(
	ctx context.Context,
	stream pb.AuctionService_EventStreamServer,
	cursor string,
) error {
	for {
		if err := ctx.Err(); err != nil {
			return status.Error(codes.Canceled, "closed stream")
		}

		streams, err := s.Redis.XRead(ctx, &redis.XReadArgs{
			Streams: []string{db.StreamKey, cursor},
			Count:   10,
			Block:   0,
		}).Result()
		if err != nil {
			slog.Error(err.Error())
			return status.Error(codes.Internal, "failed to read events")
		}

		for _, st := range streams {
			for _, msg := range st.Messages {
				rawData, err := util.ToBytes(msg.Values["data"])
				if err != nil {
					if errors.Is(err, util.TypeCastingErr) {
						slog.Error(err.Error())
						return status.Error(codes.Internal, "failed to cast event data")
					}
					return status.Error(codes.Internal, "failed to read event data")
				}

				response, err := newEventStreamResponse(msg.Values["event"], rawData)
				if err != nil {
					return err
				}

				if err := stream.Send(response); err != nil {
					return status.Error(codes.Internal, "failed to handle message")
				}

				cursor = msg.ID
			}
		}
	}
}

func newEventStreamResponse(event any, rawData []byte) (*pb.EventStreamResponse, error) {
	response := &pb.EventStreamResponse{}

	switch event {
	case db.SellEvent:
		var sellEvent pb.SellEvent
		if err := proto.Unmarshal(rawData, &sellEvent); err != nil {
			return nil, status.Error(codes.Internal, "failed to decode sell event")
		}
		response.Event = &pb.EventStreamResponse_SellEvent{SellEvent: &sellEvent}
	case db.BidEvent:
		var bidEvent pb.BidEvent
		if err := proto.Unmarshal(rawData, &bidEvent); err != nil {
			return nil, status.Error(codes.Internal, "failed to decode bid event")
		}
		response.Event = &pb.EventStreamResponse_BidEvent{BidEvent: &bidEvent}
	case db.CancelEvent:
		var cancelEvent pb.CancelEvent
		if err := proto.Unmarshal(rawData, &cancelEvent); err != nil {
			return nil, status.Error(codes.Internal, "failed to decode cancel event")
		}
		response.Event = &pb.EventStreamResponse_CancelEvent{CancelEvent: &cancelEvent}
	case db.ExpireEvent:
		var expireEvent pb.ExpireEvent
		if err := proto.Unmarshal(rawData, &expireEvent); err != nil {
			return nil, status.Error(codes.Internal, "failed to decode expire event")
		}
		response.Event = &pb.EventStreamResponse_ExpireEvent{ExpireEvent: &expireEvent}
	default:
		return nil, status.Error(codes.Internal, "invalid event type")
	}

	return response, nil
}
