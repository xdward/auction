package server

import (
	"context"
	"log/slog"

	"github.com/redis/go-redis/v9"
	pb "github.com/xdward/auction-contracts/gen/go"
	"github.com/xdward/auction/internal/auctionstore"
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
			Streams: []string{auctionstore.StreamKey, cursor},
			Count:   10,
			Block:   0,
		}).Result()
		if err != nil {
			slog.Error(err.Error())
			return status.Error(codes.Internal, "failed to read events")
		}

		for _, st := range streams {
			for _, msg := range st.Messages {
				var rawData []byte
				switch data := msg.Values["data"].(type) {
				case []byte:
					rawData = data
				case string:
					rawData = []byte(data)
				default:
					slog.Error("invalid event data type")
					return status.Error(codes.Internal, "failed to read event data")
				}

				var response pb.EventStreamResponse
				if err := proto.Unmarshal(rawData, &response); err != nil {
					return status.Error(codes.Internal, "failed to decode event")
				}

				if err := stream.Send(&response); err != nil {
					return status.Error(codes.Internal, "failed to handle message")
				}

				cursor = msg.ID
			}
		}
	}
}
