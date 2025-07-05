# RRQ Implementation TODO List

## 🚀 **IMMEDIATE PRIORITY**

### 1. **Restrict Burst Mode in Multi-Process Mode**
- **Issue**: Burst mode doesn't coordinate across multiple processes, causing confusing behavior
- **Solution**: Disable `--burst` flag when `--num-workers > 1`
- **Location**: `cli.py:worker_run_command()`
- **Implementation**:
  ```python
  if num_workers > 1 and burst:
      click.echo(click.style("ERROR: --burst mode is not supported with multiple workers (--num-workers > 1)", fg="red"), err=True)
      sys.exit(1)
  ```
- **Tests**: Add CLI test for this validation

### 2. **Implement LUA Scripts for Atomicity**
- **Priority**: High (Performance + Correctness)
- **Scripts Needed**:
  
  #### A. **Atomic Lock-and-Remove Script**
  - **Purpose**: Combine job lock acquisition + queue removal
  - **Location**: `store.py` - new method `atomic_lock_and_remove_job()`
  - **Benefits**: Eliminates race condition, reduces Redis round trips
  - **Implementation**: 
    ```lua
    -- KEYS: [1] = lock_key, [2] = queue_key
    -- ARGV: [1] = worker_id, [2] = lock_timeout_ms, [3] = job_id
    local lock_result = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2])
    if lock_result then
        local removed_count = redis.call('ZREM', KEYS[2], ARGV[3])
        if removed_count == 0 then
            redis.call('DEL', KEYS[1])  -- Release lock if job wasn't in queue
            return {0, 0}  -- {lock_acquired, removed_count}
        end
        return {1, removed_count}
    else
        return {0, 0}
    end
    ```
  
  #### B. **Atomic Retry Script**
  - **Purpose**: Combine retry count increment + status update + re-queue
  - **Location**: `store.py` - new method `atomic_retry_job()`
  - **Benefits**: Eliminates retry logic race conditions
  - **Implementation**:
    ```lua
    -- KEYS: [1] = job_key, [2] = queue_key
    -- ARGV: [1] = job_id, [2] = retry_at_score, [3] = error_message
    local new_retry_count = redis.call('HINCRBY', KEYS[1], 'current_retries', 1)
    redis.call('HMSET', KEYS[1], 'status', 'RETRYING', 'last_error', ARGV[3])
    redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
    return new_retry_count
    ```

- **Integration Points**:
  - Update `worker.py:_try_process_job()` to use atomic lock-and-remove
  - Update `worker.py:_process_retry_job()` to use atomic retry
  - Update `worker.py:_process_other_failure()` to use atomic retry

### 3. **Comprehensive Testing Suite**
- **Multi-Worker Integration Tests**:
  - Test multiple workers processing same queue
  - Verify no job duplication
  - Test race conditions around job locking
  - Test signal handling and graceful shutdown
- **LUA Script Tests**:
  - Test atomic operations under high concurrency
  - Test failure scenarios (job disappears, Redis connection issues)
- **Load Testing**:
  - High job volumes with many workers
  - Memory leak detection
  - Connection pool behavior

---

## 📋 **MEDIUM PRIORITY** (After Core Implementation)

### 4. **Process Monitoring and Health Checks**
- **Issue**: Limited subprocess monitoring in multi-worker setup
- **Location**: `cli.py:_run_multiple_workers()`
- **Implementation**:
  - Add periodic health checks for subprocess workers
  - Implement automatic restart on worker failure
  - Add worker process metrics (CPU, memory, job count)
  - Consider using a process manager like supervisord integration

### 5. **Semaphore Optimization**
- **Issue**: Semaphore acquired before lock check
- **Location**: `worker.py:_poll_for_jobs()`
- **Options**:
  - **Option A**: Acquire semaphore only after successful lock
  - **Option B**: Implement job reservation system
  - **Option C**: Use Redis-based semaphore for global concurrency control

### 6. **Jittered Poll Delays**
- **Issue**: All workers poll simultaneously (thundering herd)
- **Location**: `worker.py:_run_loop()`
- **Implementation**:
  ```python
  base_delay = self.settings.default_poll_delay_seconds
  jittered_delay = base_delay * (0.5 + random.random() * 0.5)  # ±50% jitter
  await asyncio.sleep(jittered_delay)
  ```

---

## 📝 **LOW PRIORITY** (Nice-to-Have)

### 7. **Queue Priority System**
- **Enhancement**: Add queue prioritization in round-robin polling
- **Location**: `worker.py:_poll_for_jobs()`
- **Implementation**: Priority-weighted queue selection

### 8. **Connection Pool Optimization**
- **Enhancement**: Optimize Redis connection pooling for multi-worker scenarios
- **Location**: `store.py:__init__()`
- **Investigation**: Profile connection usage patterns

### 9. **Metrics and Observability**
- **Enhancement**: Add detailed metrics for multi-worker deployments
- **Metrics**: Job processing rates, lock contention, queue depths
- **Integration**: Prometheus/OpenTelemetry support

### 10. **Advanced Signal Handling**
- **Enhancement**: More robust signal handling for edge cases
- **Location**: `cli.py:_run_multiple_workers()`
- **Implementation**: Handle SIGCHLD, process group edge cases

---

## 📚 **DOCUMENTATION UPDATES**

### 11. **Multi-Worker Documentation**
- **Update**: Document that `worker_concurrency` is per-process, not global
- **Add**: Burst mode restriction explanation
- **Add**: Recommended deployment patterns
- **Add**: Performance tuning guide

### 12. **Configuration Guide**
- **Add**: Multi-worker configuration best practices
- **Add**: Redis configuration recommendations
- **Add**: Monitoring and alerting setup

---

## 🧪 **TESTING CHECKLIST**

### Pre-Implementation Testing
- [ ] Current multi-worker functionality works correctly
- [ ] Existing tests pass with multi-worker scenarios
- [ ] Performance baseline established

### Post-LUA Script Testing
- [ ] All atomic operations work correctly
- [ ] No job duplication under high concurrency
- [ ] Performance improvement measured
- [ ] Failure scenarios handled properly

### Integration Testing
- [ ] Multiple workers + high job volume
- [ ] Redis failover scenarios
- [ ] Worker restart scenarios
- [ ] Signal handling in various environments

---

## 📅 **IMPLEMENTATION PHASES**

### Phase 1: Core Fixes (Week 1-2)
1. Burst mode restriction
2. LUA script implementation
3. Basic integration tests

### Phase 2: Stability (Week 3-4)
4. Process monitoring
5. Comprehensive testing
6. Documentation updates

### Phase 3: Optimization (Week 5-6)
7. Semaphore optimization
8. Poll delay jittering
9. Performance testing

### Phase 4: Polish (Week 7-8)
10. Advanced features
11. Metrics and observability
12. Final documentation

---

## 🔍 **SUCCESS CRITERIA**

- [ ] Zero job duplication in multi-worker scenarios
- [ ] Graceful handling of worker failures
- [ ] Performance improvement from LUA scripts
- [ ] Clear documentation for multi-worker deployment
- [ ] Comprehensive test coverage (>90%)
- [ ] Production-ready monitoring and alerting