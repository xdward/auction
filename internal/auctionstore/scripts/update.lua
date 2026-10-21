local versionKey = KEYS[1]
local streamKey = KEYS[2]
local versionToStreamKey = KEYS[3]

local newVersion = redis.call('INCR', versionKey)
local streamEntryID = redis.call('XADD', streamKey, '*',
    'data', ARGV[1],
    'version', newVersion
)

redis.call('SET', versionToStreamKey .. newVersion, streamEntryID)

return { newVersion, streamEntryID }
