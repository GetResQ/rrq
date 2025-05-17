# RRQ Multi-Worker Safety

The core coordination primitives in RRQ are already designed for safe fan-out over multiple worker processes:

- Jobs live in a Redis ZSET, with workers atomically acquiring a per-job lock (`SET NX PX`) before removing (`ZREM`) the job ID and executing it
- The lock ensures that even if two workers race on the same job, only one proceeds past the `SET NX`
- Once removed from the ZSET, the job can't re-appear until a retry or DLQ–requeue
- Heartbeats and health keys are namespaced by worker ID (PID+UUID), so many workers can register themselves independently

## Areas to Watch in Large Worker Fleets

### 1. Two-step Lock+Pop Isn't Fully Atomic
- If a worker acquires the lock but crashes before `ZREM`, the job stays in the queue until the lock TTL expires, and then another worker can grab it
- In practice it's rare (must crash in that tiny window), but you can eliminate it by bundling "pop from ZSET + set lock" in a single Lua script

### 2. Lock TTL vs. Job Duration
- We set the lock TTL = `job_timeout + default_lock_timeout_extension_seconds`. If your handlers sometimes exceed that window, the lock can expire mid-run (though the job isn't in the queue anymore)
- Consider increasing the extension, or implementing a "lock refresher" for very long tasks

### 3. Graceful Shutdown & Task Drain
- Workers will stop polling in burst mode or on a shutdown signal, then "drain" in-flight tasks up to `worker_shutdown_grace_period_seconds`
- Beyond that they cancel. Make sure your handlers handle `CancelledError` gracefully

### 4. Logging & Observability
- If you're tailing stdout/stderr from many workers, add the worker ID (and queue list) to your log formatter to keep things straight

### 5. Health-Check TTLs
- The heartbeat loop writes a Redis key with a buffer TTL
- If your network is flaky or your workers get paused (e.g. in a debugger), you may see transient "missing" health keys

## Summary

With these caveats in mind, you can absolutely spin up dozens (or hundreds) of RRQ worker processes against the same Redis instance, each pulling from the same or different queue names. The locking, queueing, and retry/DLQ logic will keep them from stepping on each other.
