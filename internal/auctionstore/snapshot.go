package auctionstore

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/redis/go-redis/v9"
	pb "github.com/xdward/auction-contracts/gen/go"
)

// GetSnapshot returns the current auction snapshot and version.
func (c *Client) GetSnapshot(ctx context.Context) (*pb.Snapshot, *string, error) {
	lua := redis.NewScript(SnapshotScript)
	out, err := lua.Run(ctx, c.rdb, []string{SortedSetKey, VersionKey}).Result()
	if err != nil {
		return nil, nil, err
	}

	// expect the script to return { listingsJSON, version }
	result, ok := out.([]any)
	if !ok || len(result) != 2 {
		return nil, nil, RedisScriptErr
	}
	listingsJSON := result[0].(string)
	version := result[1].(string)

	// parse listingsJSON into a slice of structs
	var listings []struct {
		ItemId     uint64 `json:"item_id"`
		CurrentBid uint64 `json:"current_bid"`
		Expiration string `json:"expires_at"`
	}
	if listingsJSON != "{}" {
		if err := json.Unmarshal([]byte(listingsJSON), &listings); err != nil {
			return nil, nil, err
		}
	}

	// create a slice of AuctionListing messages
	snapshotSlice := make([]*pb.AuctionListing, 0, len(listings))

	// parse each struct into an AuctionListing
	for _, l := range listings {
		p := &pb.AuctionListing{
			ItemId:     l.ItemId,
			CurrentBid: l.CurrentBid,
			Expiration: l.Expiration,
		}
		snapshotSlice = append(snapshotSlice, p)
	}

	snapshot := &pb.Snapshot{
		Listings: snapshotSlice,
		Version:  version,
	}

	// resolve the stream cursor from the snapshot version
	cursorKey := VersionToEntryPrefix + snapshot.Version
	cursor, err := c.rdb.Get(ctx, cursorKey).Result()
	if err == redis.Nil {
		cursor = "0-0"
	} else if err != nil {
		slog.Error("failed to get stream cursor",
			slog.String("error", err.Error()),
			slog.Group("snapshot",
				slog.String("version", snapshot.Version),
				slog.String("cursorKey", cursorKey),
			),
		)
		return nil, nil, errors.New("failed to get stream cursor")
	}

	return snapshot, &cursor, nil
}
