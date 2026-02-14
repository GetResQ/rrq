-- KEYS: [1] = job_key, [2] = queue_key
-- ARGV: field/value pairs..., score, job_id
if redis.call('EXISTS', KEYS[1]) == 1 then
    return 0
end
redis.call('HSET', KEYS[1], unpack(ARGV, 1, #ARGV - 2))
redis.call('ZADD', KEYS[2], ARGV[#ARGV - 1], ARGV[#ARGV])
return 1
