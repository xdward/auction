package auctionstore

import _ "embed"

//go:embed scripts/snapshot.lua
var SnapshotScript string

//go:embed scripts/update.lua
var UpdateScript string

type listings []struct {
	ItemId     uint64 `json:"item_id"`
	CurrentBid uint64 `json:"current_bid"`
	Expiration string `json:"expires_at"`
}

var snapshotScriptKeys = []string{
	SortedSetKey,
	VersionKey,
}

var updateScriptKeys = []string{
	VersionKey,
	StreamKey,
	VersionToEntryPrefix,
}
