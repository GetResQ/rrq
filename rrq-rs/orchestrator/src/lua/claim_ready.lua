-- KEYS: [1] = queue_key, [2] = active_key
-- ARGV: [1] = worker_id, [2] = now_ms, [3] = max_claims,
--       [4] = default_lock_timeout_ms, [5] = lock_timeout_extension_seconds,
--       [6] = start_time, [7] = active_score
local max_claims = tonumber(ARGV[3]) or 0
if max_claims <= 0 then
    return {}
end
local default_lock_timeout_ms = tonumber(ARGV[4]) or 0
local lock_extension_seconds = tonumber(ARGV[5]) or 0
local max_redis_px = 9223372036854775807
if default_lock_timeout_ms <= 0 then
    return {}
end
local scan_count = math.max(max_claims * 4, max_claims)
local candidates = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[2], 'LIMIT', 0, scan_count)
local claimed = {}
for _, job_id in ipairs(candidates) do
    if #claimed >= max_claims then
        break
    end
    local job_key = 'rrq:job:' .. job_id
    if redis.call('EXISTS', job_key) ~= 1 then
        redis.call('ZREM', KEYS[1], job_id)
    else
        local lock_timeout_ms = default_lock_timeout_ms
        local job_timeout_seconds = tonumber(redis.call('HGET', job_key, 'job_timeout_seconds'))
        if job_timeout_seconds ~= nil and job_timeout_seconds > 0 then
            local candidate_timeout_ms = (job_timeout_seconds + lock_extension_seconds) * 1000
            if candidate_timeout_ms > 0 and candidate_timeout_ms <= max_redis_px then
                lock_timeout_ms = candidate_timeout_ms
            end
        end
        local lock_key = 'rrq:lock:job:' .. job_id
        local lock_ok = redis.call('SET', lock_key, ARGV[1], 'NX', 'PX', lock_timeout_ms)
        if lock_ok then
            local removed = redis.call('ZREM', KEYS[1], job_id)
            if removed == 1 then
                redis.call('HSET', job_key, 'status', 'ACTIVE', 'start_time', ARGV[6], 'worker_id', ARGV[1])
                redis.call('ZADD', KEYS[2], ARGV[7], job_id)
                table.insert(claimed, job_id)
            else
                redis.call('DEL', lock_key)
            end
        end
    end
end
return claimed
