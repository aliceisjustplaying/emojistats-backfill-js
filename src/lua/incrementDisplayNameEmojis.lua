local emojis = cjson.decode(ARGV[1]) -- array of emojis

-- Increment global counters
redis.call('INCR', 'displayNamesWithEmojis')

for _, emoji in ipairs(emojis) do
  redis.call('ZINCRBY', 'displayNameStats', 1, emoji)
  redis.call('INCR', 'processedDisplayNames')
end

return 'OK'
