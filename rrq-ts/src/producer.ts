import crypto from "node:crypto";
import Redis from "ioredis";
import {
  JOB_KEY_PREFIX,
  QUEUE_KEY_PREFIX,
  UNIQUE_JOB_LOCK_PREFIX,
} from "./constants.js";
import { DEFAULT_SETTINGS, resolveSettings, RRQSettings } from "./settings.js";

export interface EnqueueOptions {
  args?: unknown[];
  kwargs?: Record<string, unknown>;
  queueName?: string;
  jobId?: string;
  uniqueKey?: string;
  maxRetries?: number;
  jobTimeoutSeconds?: number;
  resultTtlSeconds?: number;
  deferUntil?: Date;
  deferBySeconds?: number;
  traceContext?: Record<string, string> | null;
}

export interface JobRecord {
  id: string;
  function_name: string;
  job_args: string;
  job_kwargs: string;
  enqueue_time: string;
  status: "PENDING";
  current_retries: string;
  queue_name: string;
  next_scheduled_run_time: string;
  max_retries: string;
  job_timeout_seconds: string;
  result_ttl_seconds: string;
  job_unique_key?: string;
  result: string;
  trace_context?: string;
}

export class RRQClient {
  private settings: RRQSettings;
  private redis: Redis;
  private ownsRedis: boolean;

  constructor(settings?: Partial<RRQSettings>, redis?: Redis) {
    this.settings = resolveSettings(settings);
    if (redis) {
      this.redis = redis;
      this.ownsRedis = false;
    } else {
      this.redis = new Redis(this.settings.redisDsn);
      this.ownsRedis = true;
    }
  }

  async close(): Promise<void> {
    if (this.ownsRedis) {
      await this.redis.quit();
    }
  }

  async enqueue(functionName: string, options: EnqueueOptions = {}): Promise<string> {
    const args = options.args ?? [];
    const kwargs = options.kwargs ?? {};
    const queueName = options.queueName ?? this.settings.defaultQueueName;
    const jobId = options.jobId ?? crypto.randomUUID();

    const enqueueTime = new Date();
    let desiredRunTime = enqueueTime;
    let lockTtlSeconds = this.settings.defaultUniqueJobLockTtlSeconds;

    if (options.deferUntil) {
      const deferUntil = ensureUtcDate(options.deferUntil);
      desiredRunTime = deferUntil;
      const diffSeconds = Math.max(
        0,
        Math.ceil((deferUntil.getTime() - enqueueTime.getTime()) / 1000),
      );
      lockTtlSeconds = Math.max(lockTtlSeconds, diffSeconds + 1);
    } else if (typeof options.deferBySeconds === "number") {
      const deferSeconds = Math.max(0, Math.floor(options.deferBySeconds));
      desiredRunTime = new Date(enqueueTime.getTime() + deferSeconds * 1000);
      lockTtlSeconds = Math.max(lockTtlSeconds, deferSeconds + 1);
    }

    let uniqueAcquired = false;
    if (options.uniqueKey) {
      const remainingTtl = await this.getLockTtl(options.uniqueKey);
      if (remainingTtl > 0) {
        desiredRunTime = new Date(
          Math.max(
            desiredRunTime.getTime(),
            enqueueTime.getTime() + remainingTtl * 1000,
          ),
        );
      } else {
        uniqueAcquired = await this.acquireUniqueJobLock(
          options.uniqueKey,
          jobId,
          lockTtlSeconds,
        );
        if (!uniqueAcquired) {
          const remaining = await this.getLockTtl(options.uniqueKey);
          desiredRunTime = new Date(
            Math.max(
              desiredRunTime.getTime(),
              enqueueTime.getTime() + remaining * 1000,
            ),
          );
        }
      }
    }

    const jobRecord: JobRecord = {
      id: jobId,
      function_name: functionName,
      job_args: JSON.stringify(args),
      job_kwargs: JSON.stringify(kwargs),
      enqueue_time: enqueueTime.toISOString(),
      status: "PENDING",
      current_retries: "0",
      queue_name: queueName,
      next_scheduled_run_time: desiredRunTime.toISOString(),
      max_retries: String(options.maxRetries ?? this.settings.defaultMaxRetries),
      job_timeout_seconds: String(
        options.jobTimeoutSeconds ?? this.settings.defaultJobTimeoutSeconds,
      ),
      result_ttl_seconds: String(
        options.resultTtlSeconds ?? this.settings.defaultResultTtlSeconds,
      ),
      result: JSON.stringify(null),
    };

    if (options.uniqueKey) {
      jobRecord.job_unique_key = options.uniqueKey;
    }
    if (options.traceContext) {
      jobRecord.trace_context = JSON.stringify(options.traceContext);
    }

    const jobKey = `${JOB_KEY_PREFIX}${jobId}`;
    const queueKey = formatQueueKey(queueName);
    const score = desiredRunTime.getTime();

    try {
      const pipeline = this.redis.pipeline();
      pipeline.hset(jobKey, jobRecord);
      pipeline.zadd(queueKey, score, jobId);
      await pipeline.exec();
    } catch (error) {
      if (uniqueAcquired && options.uniqueKey) {
        await this.releaseUniqueJobLock(options.uniqueKey);
      }
      throw error;
    }

    return jobId;
  }

  private async acquireUniqueJobLock(
    uniqueKey: string,
    jobId: string,
    ttlSeconds: number,
  ): Promise<boolean> {
    const lockKey = `${UNIQUE_JOB_LOCK_PREFIX}${uniqueKey}`;
    const result = await this.redis.set(lockKey, jobId, "EX", ttlSeconds, "NX");
    return result === "OK";
  }

  private async releaseUniqueJobLock(uniqueKey: string): Promise<void> {
    const lockKey = `${UNIQUE_JOB_LOCK_PREFIX}${uniqueKey}`;
    await this.redis.del(lockKey);
  }

  private async getLockTtl(uniqueKey: string): Promise<number> {
    const lockKey = `${UNIQUE_JOB_LOCK_PREFIX}${uniqueKey}`;
    const ttl = await this.redis.ttl(lockKey);
    return typeof ttl === "number" && ttl > 0 ? ttl : 0;
  }
}

export function formatQueueKey(queueName: string): string {
  if (queueName.startsWith(QUEUE_KEY_PREFIX)) {
    return queueName;
  }
  return `${QUEUE_KEY_PREFIX}${queueName}`;
}

function ensureUtcDate(value: Date): Date {
  if (Number.isNaN(value.getTime())) {
    throw new Error("Invalid deferUntil timestamp");
  }
  return value;
}

export { DEFAULT_SETTINGS };
