package auctionstore

const (
	ListingKeyPrefix     string = "auction:listing:"
	SortedSetKey         string = "auction:listing:recent"
	CancelledSetKey      string = "auction:listing:cancelled"
	VersionKey           string = "auction:version"
	StreamKey            string = "auction:stream:events"
	VersionToEntryPrefix string = "auction:version_to_id:"
)
