package db

import (
	"strconv"
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
