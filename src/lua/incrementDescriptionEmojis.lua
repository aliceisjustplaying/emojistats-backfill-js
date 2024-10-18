local emojis = cjson.decode(ARGV[1]) -- array of emojis

-- Increment global counters
redis.call('INCR', 'descriptionsWithEmojis')

for _, emoji in ipairs(emojis) do
  redis.call('ZINCRBY', 'descriptionStats', 1, emoji)
  redis.call('INCR', 'processedDescriptions')
end

return 'OK'
