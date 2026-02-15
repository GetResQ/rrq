-- KEYS: [1] = lock_key
-- ARGV: [1] = worker_id
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
redis.call('DEL', KEYS[1])
return 1
