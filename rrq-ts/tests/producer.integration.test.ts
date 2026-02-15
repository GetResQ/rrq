import { afterEach, beforeEach, describe, expect, it } from "bun:test";

import { createClient, type RedisClientType } from "redis";

import { getProducerConstants } from "../src/producer_ffi.js";
import { RRQClient } from "../src/producer.js";

const redisUrl = process.env.RRQ_TEST_REDIS_DSN ?? "redis://localhost:6379/4";
const CONSTANTS = getProducerConstants() as {
  job_key_prefix: string;
  idempotency_key_prefix: string;
  queue_key_prefix: string;
};
const TEST_QUEUE_NAME = "ts_default";
const TEST_QUEUE_KEY = `${CONSTANTS.queue_key_prefix}${TEST_QUEUE_NAME}`;

let redis: RedisClientType;
let client: RRQClient;

describe("RRQClient producer integration", () => {
  beforeEach(async () => {
    redis = createClient({ url: redisUrl });
    await redis.connect();
    await redis.flushDb();
    client = new RRQClient({
      config: {
        redisDsn: redisUrl,
        queueName: TEST_QUEUE_NAME,
      },
    });
  });

  afterEach(async () => {
    await client.close();
    await redis.flushDb();
    await redis.quit();
  });

  it("enqueues job and stores it in Redis", async () => {
    const jobId = await client.enqueue("handler", {
      params: { n: 1, ok: true },
    });

    const jobKey = `${CONSTANTS.job_key_prefix}${jobId}`;
    const status = await redis.hGet(jobKey, "status");
    expect(status).toBe("PENDING");
    const queueName = await redis.hGet(jobKey, "queue_name");
    expect(queueName).toBe(TEST_QUEUE_KEY);

    const score = await redis.zScore(TEST_QUEUE_KEY, jobId);
    expect(score).not.toBeNull();
  });

  it("schedules deferred jobs in the future", async () => {
    const delaySeconds = 5;
    const startMs = Date.now();
    const jobId = await client.enqueueDeferred("handler", { deferBySeconds: delaySeconds });

    const score = await redis.zScore(TEST_QUEUE_KEY, jobId);
    expect(score).not.toBeNull();
    const minScore = startMs + delaySeconds * 1000 - 1000;
    expect(score as number).toBeGreaterThanOrEqual(minScore);
  });

  it("uses explicit enqueueTime when provided", async () => {
    const enqueueTime = new Date("2024-01-01T00:00:00.000Z");
    const jobId = await client.enqueue("handler", { enqueueTime });
    const jobKey = `${CONSTANTS.job_key_prefix}${jobId}`;
    const stored = await redis.hGet(jobKey, "enqueue_time");
    expect(stored).not.toBeNull();
    expect(Date.parse(stored as string)).toBe(enqueueTime.getTime());
  });

  it("reuses job id for unique keys", async () => {
    const uniqueKey = "user-unique-1";
    const job1 = await client.enqueue("handler", { uniqueKey });
    const job2 = await client.enqueue("handler", { uniqueKey });
    expect(job2).toBe(job1);

    const idemValue = await redis.get(`${CONSTANTS.idempotency_key_prefix}${uniqueKey}`);
    expect(idemValue).not.toBeNull();
  });

  it("returns null when rate limited", async () => {
    const key = "user-rate-1";
    const job1 = await client.enqueueWithRateLimit("handler", {
      rateLimitKey: key,
      rateLimitSeconds: 5,
    });
    expect(job1).not.toBeNull();

    const job2 = await client.enqueueWithRateLimit("handler", {
      rateLimitKey: key,
      rateLimitSeconds: 5,
    });
    expect(job2).toBeNull();
  });

  it("preserves prefixed queue names", async () => {
    const prefixedQueue = `${CONSTANTS.queue_key_prefix}prefixed_queue`;
    const jobId = await client.enqueue("handler", {
      queueName: prefixedQueue,
    });

    const jobKey = `${CONSTANTS.job_key_prefix}${jobId}`;
    const queueName = await redis.hGet(jobKey, "queue_name");
    expect(queueName).toBe(prefixedQueue);
    const score = await redis.zScore(prefixedQueue, jobId);
    expect(score).not.toBeNull();
  });

  it("returns job status when present", async () => {
    const jobId = "job-status-1";
    const jobKey = `${CONSTANTS.job_key_prefix}${jobId}`;
    const payload = { ok: true };
    await redis.hSet(jobKey, {
      status: "COMPLETED",
      result: JSON.stringify(payload),
    });

    const status = await client.getJobStatus(jobId);
    expect(status).not.toBeNull();
    expect(status?.status).toBe("COMPLETED");
    expect(status?.result).toEqual(payload);
  });
});
