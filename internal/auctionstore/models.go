package auctionstore

import "strconv"

// Redis key prefixes and shared keys used by the auction store.
const (
	ListingKeyPrefix     string = "auction:listing:"
	SortedSetKey         string = "auction:listing:recent"
	VersionKey           string = "auction:version"
	StreamKey            string = "auction:stream:events"
	VersionToEntryPrefix string = "auction:version_to_id:"
)

// Listing mirrors the Redis hash stored for each auction listing.
type Listing struct {
	Item      uint64 `redis:"item"`
	Seller    uint64 `redis:"seller"`
	Bid       uint64 `redis:"bid"`
	Bidder    uint64 `redis:"bidder"`
	CreatedAt string `redis:"created_at"`
	ExpiresAt string `redis:"expires_at"`
	Active    bool   `redis:"active"`
}

// ListingKey returns the Redis key for a listing ID.
func ListingKey(id uint64) string {
	return ListingKeyPrefix + strconv.FormatUint(id, 10)
}
