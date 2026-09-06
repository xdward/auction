package auctionstore

import (
	"context"

	"github.com/redis/go-redis/v9"
	pb "github.com/xdward/auction-contracts/gen/go"
	"google.golang.org/protobuf/proto"
)

// decodeStreamData decodes the Redis stream's "data" field into raw bytes so the event payload can
// be unmarshaled to a protobuf message.
func decodeStreamData(msg *redis.XMessage) ([]byte, error) {
	raw, ok := msg.Values["data"]
	if !ok {
		return nil, StreamDataErr
	}

	switch data := raw.(type) {
	case []byte:
		return data, nil
	case string:
		return []byte(data), nil
	default:
		return nil, StreamDecodeErr
	}
}

// BatchReadStream reads N (batchSize) events from the stream starting at the provided cursor and
// returns the decoded events with the next cursor value.
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
		return nil, nil, err
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
				return nil, nil, err
			}

			events = append(events, &response)
			newCursor = msg.ID
		}
	}

	return events, &newCursor, nil
}
