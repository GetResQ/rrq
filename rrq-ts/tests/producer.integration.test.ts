import { afterEach, beforeEach, describe, expect, it } from "bun:test";

import { createClient, type RedisClientType } from "redis";

import { DEFAULT_QUEUE_NAME, IDEMPOTENCY_KEY_PREFIX, JOB_KEY_PREFIX } from "../src/constants.js";
import { RRQClient } from "../src/producer.js";

const redisUrl = process.env.RRQ_TEST_REDIS_DSN ?? "redis://localhost:6379/4";

let redis: RedisClientType;
let client: RRQClient;

describe("RRQClient producer integration", () => {
  beforeEach(async () => {
    redis = createClient({ url: redisUrl });
    await redis.connect();
    await redis.flushDb();
    client = new RRQClient({ redisDsn: redisUrl });
  });

  afterEach(async () => {
    await client.close();
    await redis.flushDb();
    await redis.quit();
  });

  it("enqueues job and stores it in Redis", async () => {
    const jobId = await client.enqueue("handler", {
      args: [1],
      kwargs: { ok: true },
    });

    const jobKey = `${JOB_KEY_PREFIX}${jobId}`;
    const status = await redis.hGet(jobKey, "status");
    expect(status).toBe("PENDING");

    const score = await redis.zScore(DEFAULT_QUEUE_NAME, jobId);
    expect(score).not.toBeNull();
  });

  it("schedules deferred jobs in the future", async () => {
    const delaySeconds = 5;
    const startMs = Date.now();
    const jobId = await client.enqueueDeferred("handler", { deferBySeconds: delaySeconds });

    const score = await redis.zScore(DEFAULT_QUEUE_NAME, jobId);
    expect(score).not.toBeNull();
    const minScore = startMs + delaySeconds * 1000 - 1000;
    expect(score as number).toBeGreaterThanOrEqual(minScore);
  });

  it("reuses job id for unique keys", async () => {
    const uniqueKey = "user-unique-1";
    const job1 = await client.enqueue("handler", { uniqueKey });
    const job2 = await client.enqueue("handler", { uniqueKey });
    expect(job2).toBe(job1);

    const idemValue = await redis.get(`${IDEMPOTENCY_KEY_PREFIX}${uniqueKey}`);
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
});
