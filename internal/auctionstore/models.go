package auctionstore

import "strconv"

// Listing mirrors the Redis hash stored for each auction listing.
type Listing struct {
	Item      uint64 `redis:"item"`
	Seller    uint64 `redis:"seller"`
	Bid       uint64 `redis:"bid"`
	Bidder    uint64 `redis:"bidder"`
	CreatedAt string `redis:"created_at"`
	ExpiresAt string `redis:"expires_at"`
}

// ListingKey returns the Redis key for a listing ID.
func ListingKey(id uint64) string {
	return ListingKeyPrefix + strconv.FormatUint(id, 10)
}
