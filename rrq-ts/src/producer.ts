import { z } from "zod";

import { RustProducer, RustProducerError } from "./producer_ffi.js";
import { DEFAULT_SETTINGS, resolveSettings, RRQSettings } from "./settings.js";

export interface EnqueueOptions {
  args?: unknown[];
  kwargs?: Record<string, unknown>;
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
    unique_ttl_seconds: z.number().int().positive().optional(),
    max_retries: z.number().int().nonnegative().optional(),
    job_timeout_seconds: z.number().int().positive().optional(),
    result_ttl_seconds: z.number().int().nonnegative().optional(),
    trace_context: z.record(z.string()).optional(),
    defer_until: z.string().optional(),
    defer_by_seconds: z.number().finite().min(0).optional(),
    rate_limit_key: z.string().optional(),
    rate_limit_seconds: z.number().finite().positive().optional(),
    debounce_key: z.string().optional(),
    debounce_seconds: z.number().finite().positive().optional(),
  })
  .strict();

const ProducerRequestSchema = z
  .object({
    mode: z.string(),
    function_name: z.string().min(1),
    args: z.array(z.any()),
    kwargs: z.record(z.any()),
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
  private settings: RRQSettings;
  private producer: RustProducer;
  private settingsOverrides: Partial<RRQSettings>;

  constructor(settings?: Partial<RRQSettings>, producer?: RustProducer) {
    this.settingsOverrides = settings ?? {};
    this.settings = resolveSettings(this.settingsOverrides);
    this.producer =
      producer ??
      RustProducer.fromConfig({
        redis_dsn: this.settings.redisDsn,
        ...(this.settingsOverrides.defaultQueueName !== undefined
          ? { queue_name: this.settings.defaultQueueName }
          : {}),
        ...(this.settingsOverrides.defaultMaxRetries !== undefined
          ? { max_retries: this.settings.defaultMaxRetries }
          : {}),
        ...(this.settingsOverrides.defaultJobTimeoutSeconds !== undefined
          ? { job_timeout_seconds: this.settings.defaultJobTimeoutSeconds }
          : {}),
        ...(this.settingsOverrides.defaultResultTtlSeconds !== undefined
          ? { result_ttl_seconds: this.settings.defaultResultTtlSeconds }
          : {}),
        ...(this.settingsOverrides.defaultUniqueJobLockTtlSeconds !== undefined
          ? { idempotency_ttl_seconds: this.settings.defaultUniqueJobLockTtlSeconds }
          : {}),
      });
  }

  async close(): Promise<void> {
    this.producer.close();
  }

  async enqueue(functionName: string, options: EnqueueOptions = {}): Promise<string> {
    const mode =
      options.uniqueKey !== undefined && options.uniqueKey !== null ? "unique" : "enqueue";
    const response = await this.callProducer(mode, functionName, options);
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
    const response = await this.callProducer("rate_limit", functionName, options);
    return response.job_id ?? null;
  }

  async enqueueWithDebounce(functionName: string, options: DebounceOptions): Promise<string> {
    const response = await this.callProducer("debounce", functionName, options, {
      includeDefers: false,
    });
    return this.expectJobId(response);
  }

  async enqueueDeferred(functionName: string, options: EnqueueOptions): Promise<string> {
    return this.enqueue(functionName, options);
  }

  private async callProducer(
    mode: string,
    functionName: string,
    options: EnqueueOptions,
    config: { includeDefers?: boolean } = {},
  ): Promise<ProducerResponse> {
    const request = {
      mode,
      function_name: functionName,
      args: options.args ?? [],
      kwargs: options.kwargs ?? {},
      options: this.buildOptions(options, config),
    };

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

  private buildOptions(
    options: EnqueueOptions,
    config: { includeDefers?: boolean },
  ): Record<string, unknown> {
    const includeDefers = config.includeDefers ?? true;
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

    if (includeDefers) {
      if (options.deferUntil) {
        payload.defer_until = formatDeferUntil(options.deferUntil);
      }
      if (options.deferBySeconds !== undefined) {
        if (!Number.isFinite(options.deferBySeconds)) {
          throw new RustProducerError("deferBySeconds must be finite");
        }
        payload.defer_by_seconds = Math.max(0, options.deferBySeconds);
      }
    }

    if ("rateLimitKey" in options) {
      if (!Number.isFinite((options as RateLimitOptions).rateLimitSeconds)) {
        throw new RustProducerError("rateLimitSeconds must be finite");
      }
      payload.rate_limit_key = (options as RateLimitOptions).rateLimitKey;
      payload.rate_limit_seconds = (options as RateLimitOptions).rateLimitSeconds;
    }

    if ("debounceKey" in options) {
      if (!Number.isFinite((options as DebounceOptions).debounceSeconds)) {
        throw new RustProducerError("debounceSeconds must be finite");
      }
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

export { DEFAULT_SETTINGS };
export { RustProducerError };

function formatDeferUntil(value: Date): string {
  if (Number.isNaN(value.getTime())) {
    throw new RustProducerError("Invalid deferUntil timestamp");
  }
  return value.toISOString();
}
