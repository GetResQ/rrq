-- KEYS: [1] = job_key, [2] = queue_key
-- ARGV: [1] = job_id, [2] = retry_at_score, [3] = error_message, [4] = status
local new_retry_count = redis.call('HINCRBY', KEYS[1], 'current_retries', 1)
redis.call('HMSET', KEYS[1], 'status', ARGV[4], 'last_error', ARGV[3])
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
return new_retry_count
