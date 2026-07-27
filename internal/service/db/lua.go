package db

const SnapshotScript = `
local sortedSetKey = KEYS[1]
local versionKey = KEYS[2]

local listingKeys = redis.call('ZRANGE', sortedSetKey, 0, -1)
local listings = {}
for i=1, #listingKeys do
	local key = listingKeys[i]
	if redis.call('HGET', key, 'active') == '1' then
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

var UpdateScriptKeys = []string{
	VersionKey,
	StreamKey,
	VersionToEntryPrefix,
}
