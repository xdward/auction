package auctionstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"
	pb "github.com/xdward/auction-contracts/gen/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func decodeStreamData(msg *redis.XMessage) ([]byte, error) {
	raw, ok := msg.Values["data"]
	if !ok {
		return nil, fmt.Errorf("missing data field")
	}

	switch data := raw.(type) {
	case []byte:
		return data, nil
	case string:
		return []byte(data), nil
	default:
		return nil, errors.New("failed to decode stream data")
	}
}

func (c *Client) BatchReadStream(
	ctx context.Context,
	batchSize int64,
	cursor string,
) ([]*pb.EventStreamResponse, *string, error) {
	streams, err := c.rdb.XRead(ctx, &redis.XReadArgs{
		Streams: []string{StreamKey, cursor},
		Count:   batchSize,
		Block:   0,
	}).Result()
	if err != nil {
		slog.Error(err.Error())
		return nil, nil, status.Error(codes.Internal, "failed to read events")
	}

	events := make([]*pb.EventStreamResponse, 0, batchSize)
	newCursor := cursor

	for _, st := range streams {
		for _, msg := range st.Messages {
			rawData, err := decodeStreamData(&msg)
			if err != nil {
				return nil, nil, err
			}

			var response pb.EventStreamResponse
			if err := proto.Unmarshal(rawData, &response); err != nil {
				return nil, nil, status.Error(codes.Internal, "failed to decode event")
			}

			events = append(events, &response)

			newCursor = msg.ID
		}
	}

	return events, &newCursor, nil
}
