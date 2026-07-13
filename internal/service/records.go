package service

import "fmt"

const (
	RedisPrefix      = "auction:listing:"
	ListingKeyFormat = RedisPrefix + "%d"     // Formatted Key -> Listing
	ListingInsertion = RedisPrefix + "recent" // Key -> Sorted Set
)

type Listing struct {
	Item      uint64 `redis:"item"`
	Seller    uint64 `redis:"seller"`
	Bid       uint64 `redis:"bid"`
	Bidder    uint64 `redis:"bidder"`
	CreatedAt int64  `redis:"created_at"`
	ExpiresAt int64  `redis:"expires_at"`
	Active    bool   `redis:"active"`
}

func BuildListingKey(id uint64) string {
	return fmt.Sprintf(ListingKeyFormat, id)
}
