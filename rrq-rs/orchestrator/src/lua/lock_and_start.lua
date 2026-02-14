-- KEYS: [1] = lock_key, [2] = queue_key, [3] = job_key, [4] = active_key
-- ARGV: [1] = worker_id, [2] = lock_timeout_ms, [3] = job_id, [4] = start_time, [5] = active_score
local lock_result = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2])
if lock_result then
    local removed_count = redis.call('ZREM', KEYS[2], ARGV[3])
    if removed_count == 0 then
        redis.call('DEL', KEYS[1])
        return {0, 0}
    end
    redis.call('HSET', KEYS[3], 'status', 'ACTIVE', 'start_time', ARGV[4], 'worker_id', ARGV[1])
    redis.call('ZADD', KEYS[4], ARGV[5], ARGV[3])
    return {1, removed_count}
else
    return {0, 0}
end
