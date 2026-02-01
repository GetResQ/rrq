import { z } from "zod";

import { JobResult, JobStatusResponse, RustProducer, RustProducerError } from "./producer_ffi.js";

export interface ProducerConfig {
  redisDsn: string;
  queueName?: string;
  maxRetries?: number;
  jobTimeoutSeconds?: number;
  resultTtlSeconds?: number;
  idempotencyTtlSeconds?: number;
}

export interface EnqueueOptions {
  params?: Record<string, unknown>;
  queueName?: string;
  jobId?: string;
  uniqueKey?: string;
  uniqueTtlSeconds?: number;
  maxRetries?: number;
  jobTimeoutSeconds?: number;
  resultTtlSeconds?: number;
  deferUntil?: Date;
  deferBySeconds?: number;
  traceContext?: Record<string, string> | null;
}

export interface RateLimitOptions extends EnqueueOptions {
  rateLimitKey: string;
  rateLimitSeconds: number;
}

export interface DebounceOptions extends EnqueueOptions {
  debounceKey: string;
  debounceSeconds: number;
}

interface ProducerResponse {
  status?: string;
  job_id?: string | null;
}

const ProducerOptionsSchema = z
  .object({
    queue_name: z.string().optional(),
    job_id: z.string().optional(),
    unique_key: z.string().optional(),
    unique_ttl_seconds: z.number().int().optional(),
    max_retries: z.number().int().optional(),
    job_timeout_seconds: z.number().int().optional(),
    result_ttl_seconds: z.number().int().optional(),
    trace_context: z.record(z.string()).optional(),
    defer_until: z.string().optional(),
    defer_by_seconds: z.number().optional(),
    rate_limit_key: z.string().optional(),
    rate_limit_seconds: z.number().optional(),
    debounce_key: z.string().optional(),
    debounce_seconds: z.number().optional(),
  })
  .strict();

const ProducerRequestSchema = z
  .object({
    mode: z.string().optional(),
    function_name: z.string().min(1),
    params: z.record(z.any()),
    options: ProducerOptionsSchema,
  })
  .strict();

const ProducerResponseSchema = z
  .object({
    status: z.enum(["enqueued", "rate_limited"]).optional(),
    job_id: z.string().nullable().optional(),
  })
  .strict();

export class RRQClient {
  private producer: RustProducer;

  constructor(
    options: { config?: ProducerConfig; configPath?: string; producer?: RustProducer } = {},
  ) {
    const { config, configPath, producer } = options;
    if (config && configPath) {
      throw new RustProducerError("Provide either config or configPath, not both.");
    }
    if (producer) {
      this.producer = producer;
      return;
    }
    if (config) {
      this.producer = RustProducer.fromConfig(toProducerConfig(config));
      return;
    }
    this.producer = RustProducer.fromToml(configPath);
  }

  async close(): Promise<void> {
    this.producer.close();
  }

  async enqueue(functionName: string, options: EnqueueOptions = {}): Promise<string> {
    const response = await this.callProducer(functionName, options);
    return this.expectJobId(response);
  }

  async enqueueWithUniqueKey(
    functionName: string,
    uniqueKey: string,
    options: Omit<EnqueueOptions, "uniqueKey"> = {},
  ): Promise<string> {
    return this.enqueue(functionName, { ...options, uniqueKey });
  }

  async enqueueWithRateLimit(
    functionName: string,
    options: RateLimitOptions,
  ): Promise<string | null> {
    const response = await this.callProducer(functionName, options);
    return response.job_id ?? null;
  }

  async enqueueWithDebounce(functionName: string, options: DebounceOptions): Promise<string> {
    const response = await this.callProducer(functionName, options);
    return this.expectJobId(response);
  }

  async enqueueDeferred(functionName: string, options: EnqueueOptions): Promise<string> {
    return this.enqueue(functionName, options);
  }

  async getJobStatus(jobId: string): Promise<JobResult | null> {
    const response = (await this.producer.getJobStatus({
      job_id: jobId,
    })) as JobStatusResponse;
    if (!response.found || !response.job) {
      return null;
    }
    return response.job;
  }

  private async callProducer(
    functionName: string,
    options: EnqueueOptions,
    mode?: string,
  ): Promise<ProducerResponse> {
    const request: Record<string, unknown> = {
      function_name: functionName,
      params: options.params ?? {},
      options: this.buildOptions(options),
    };
    if (mode) {
      request.mode = mode;
    }

    try {
      ProducerRequestSchema.parse(request);
      const response = (await this.producer.enqueue(request)) as ProducerResponse;
      const parsed = ProducerResponseSchema.safeParse(response);
      if (!parsed.success) {
        throw new RustProducerError(parsed.error.message);
      }
      return parsed.data;
    } catch (error) {
      if (error instanceof RustProducerError) {
        throw error;
      }
      throw new RustProducerError(String(error));
    }
  }

  private buildOptions(options: EnqueueOptions): Record<string, unknown> {
    const payload: Record<string, unknown> = {
      queue_name: options.queueName,
      job_id: options.jobId,
      unique_key: options.uniqueKey,
      unique_ttl_seconds: options.uniqueTtlSeconds,
      max_retries: options.maxRetries,
      job_timeout_seconds: options.jobTimeoutSeconds,
      result_ttl_seconds: options.resultTtlSeconds,
      trace_context: options.traceContext ?? undefined,
    };

    if (options.deferUntil) {
      payload.defer_until = formatDeferUntil(options.deferUntil);
    }
    if (options.deferBySeconds !== undefined) {
      payload.defer_by_seconds = options.deferBySeconds;
    }

    if ("rateLimitKey" in options) {
      payload.rate_limit_key = (options as RateLimitOptions).rateLimitKey;
      payload.rate_limit_seconds = (options as RateLimitOptions).rateLimitSeconds;
    }

    if ("debounceKey" in options) {
      payload.debounce_key = (options as DebounceOptions).debounceKey;
      payload.debounce_seconds = (options as DebounceOptions).debounceSeconds;
    }

    return payload;
  }

  private expectJobId(response: ProducerResponse): string {
    const jobId = response.job_id;
    if (!jobId) {
      throw new RustProducerError("Producer did not return a job_id");
    }
    return jobId;
  }
}

export { RustProducerError };

function formatDeferUntil(value: Date): string {
  if (Number.isNaN(value.getTime())) {
    throw new RustProducerError("Invalid deferUntil timestamp");
  }
  return value.toISOString();
}

function toProducerConfig(config: ProducerConfig): Record<string, unknown> {
  return {
    redis_dsn: config.redisDsn,
    queue_name: config.queueName,
    max_retries: config.maxRetries,
    job_timeout_seconds: config.jobTimeoutSeconds,
    result_ttl_seconds: config.resultTtlSeconds,
    idempotency_ttl_seconds: config.idempotencyTtlSeconds,
  };
}
