package service

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/redis/go-redis/v9"
	pb "github.com/xdward/auction-contracts/gen/go"
)

const (
	ListingKeyPrefix     = "auction:listing:"
	SortedSetKey         = "auction:listing:recent"
	VersionKey           = "auction:version"
	StreamKey            = "auction:stream:events"
	VersionToEntryPrefix = "auction:version_to_id:"

	SellEvent   = "sell_event"
	BidEvent    = "bid_event"
	CancelEvent = "cancel_event"
	ExpireEvent = "expire_event"
)

type Listing struct {
	Item      uint64 `redis:"item"`
	Seller    uint64 `redis:"seller"`
	Bid       uint64 `redis:"bid"`
	Bidder    uint64 `redis:"bidder"`
	CreatedAt string `redis:"created_at"`
	ExpiresAt string `redis:"expires_at"`
	Active    bool   `redis:"active"`
}

func BuildListingKey(id uint64) string {
	return ListingKeyPrefix + strconv.FormatUint(id, 10)
}

func GetSnapshot(ctx context.Context, rdb *redis.Client) (*pb.Snapshot, error) {
	// execute snapshot script
	lua := redis.NewScript(SnapshotScript)
	out, err := lua.Run(ctx, rdb, []string{
		SortedSetKey,
		VersionKey,
	}).Result()
	if err != nil {
		return nil, err
	}

	// expect the script to return { listingsJSON, version }
	result, ok := out.([]any)
	if !ok || len(result) != 2 {
		return nil, RedisScriptErr
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
			return nil, err
		}
	}

	// create a slice of AuctionListing messages
	snapshot := make([]*pb.AuctionListing, 0, len(listings))

	// parse each struct into an AuctionListing
	for _, l := range listings {
		p := &pb.AuctionListing{
			ItemId:     l.ItemId,
			CurrentBid: l.CurrentBid,
			Expiration: l.Expiration,
		}
		snapshot = append(snapshot, p)
	}

	// return current snapshot and version number
	return &pb.Snapshot{
		Listings: snapshot,
		Version:  version,
	}, nil
}
