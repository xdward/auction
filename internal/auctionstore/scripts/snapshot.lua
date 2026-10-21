local sortedSetKey = KEYS[1]
local versionKey = KEYS[2]

local listingKeys = redis.call('ZRANGE', sortedSetKey, 0, -1)
local listings = {}
for i = 1, #listingKeys do
    local key = listingKeys[i]
    if redis.call('EXISTS', key) == 1 then
        listings[#listings + 1] = {
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
