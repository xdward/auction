package auctionstore

// SnapshotScript reads the current listings from Redis and returns them as JSON, along with the
// current store version.
const SnapshotScript = `
local sortedSetKey = KEYS[1]
local versionKey = KEYS[2]

local listingKeys = redis.call('ZRANGE', sortedSetKey, 0, -1)
local listings = {}
for i=1, #listingKeys do
	local key = listingKeys[i]
	if redis.call('EXISTS', key) == 1 then
		listings[#listings+1] = {
			item_id = tonumber(redis.call('HGET', key, 'item')),
			current_bid = tonumber(redis.call('HGET', key, 'bid')),
			expires_at = redis.call('HGET', key, 'expires_at')
		}
	end
end

local version = redis.call('GET', versionKey)
if not version then
	version = "0"
end

return { cjson.encode(listings), version }
`

// UpdateScript increments the version, appends a new event to the update stream, and stores a
// new entry in the lookup table: the version mapped to the stream entry id.
const UpdateScript = `
local versionKey = KEYS[1]
local streamKey = KEYS[2]
local versionToStreamKey = KEYS[3]

local newVersion = redis.call('INCR', versionKey)
local streamEntryID = redis.call('XADD', streamKey, '*',
	'data',  ARGV[1],
	'version', newVersion
)

redis.call('SET', versionToStreamKey .. newVersion, streamEntryID)

return { newVersion, streamEntryID }
`

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
