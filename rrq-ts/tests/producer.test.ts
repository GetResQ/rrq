import { describe, expect, it } from "bun:test";

import { RRQClient } from "../src/producer.js";
import { RustProducer, RustProducerError } from "../src/producer_ffi.js";

class StubProducer {
  lastRequest: Record<string, unknown> | null = null;
  response: Record<string, unknown>;

  constructor(response: Record<string, unknown>) {
    this.response = response;
  }

  async enqueue(request: Record<string, unknown>): Promise<Record<string, unknown>> {
    this.lastRequest = request;
    return this.response;
  }

  close(): void {}
}

describe("RRQClient producer requests", () => {
  it("omits mode when uniqueKey is provided", async () => {
    const stub = new StubProducer({ job_id: "job-1" });
    const client = new RRQClient({ producer: stub as unknown as RustProducer });

    await client.enqueue("handler", { uniqueKey: "user-1" });

    expect(stub.lastRequest?.mode).toBeUndefined();
    expect((stub.lastRequest?.options as any)?.unique_key).toBe("user-1");
  });

  it("returns null when rate limited", async () => {
    const stub = new StubProducer({ status: "rate_limited" });
    const client = new RRQClient({ producer: stub as unknown as RustProducer });

    const result = await client.enqueueWithRateLimit("handler", {
      rateLimitKey: "user-1",
      rateLimitSeconds: 5,
    });

    expect(result).toBeNull();
    const options = (stub.lastRequest?.options as any) ?? {};
    expect(options.rate_limit_key).toBe("user-1");
    expect(options.rate_limit_seconds).toBe(5);
  });

  it("sends debounce fields without client-side deferral pruning", async () => {
    const stub = new StubProducer({ job_id: "job-2" });
    const client = new RRQClient({ producer: stub as unknown as RustProducer });

    await client.enqueueWithDebounce("handler", {
      debounceKey: "user-2",
      debounceSeconds: 3,
      deferBySeconds: 10,
    });

    expect(stub.lastRequest?.mode).toBeUndefined();
    const options = (stub.lastRequest?.options as any) ?? {};
    expect(options.debounce_key).toBe("user-2");
    expect(options.debounce_seconds).toBe(3);
    expect(options.defer_by_seconds).toBe(10);
  });

  it("throws if producer returns no job_id", async () => {
    const stub = new StubProducer({});
    const client = new RRQClient({ producer: stub as unknown as RustProducer });

    await expect(client.enqueue("handler")).rejects.toBeInstanceOf(RustProducerError);
  });

  it("rejects invalid deferUntil timestamps", async () => {
    const stub = new StubProducer({ job_id: "job-3" });
    const client = new RRQClient({ producer: stub as unknown as RustProducer });

    await expect(
      client.enqueue("handler", { deferUntil: new Date("invalid") }),
    ).rejects.toBeInstanceOf(RustProducerError);
  });

  it("includes enqueue_time when enqueueTime is provided", async () => {
    const stub = new StubProducer({ job_id: "job-5" });
    const client = new RRQClient({ producer: stub as unknown as RustProducer });
    const enqueueTime = new Date("2024-01-01T00:00:00.000Z");

    await client.enqueue("handler", { enqueueTime });

    const options = (stub.lastRequest?.options as any) ?? {};
    expect(options.enqueue_time).toBe(enqueueTime.toISOString());
  });

  it("rejects invalid enqueueTime timestamps", async () => {
    const stub = new StubProducer({ job_id: "job-6" });
    const client = new RRQClient({ producer: stub as unknown as RustProducer });

    await expect(
      client.enqueue("handler", { enqueueTime: new Date("invalid") }),
    ).rejects.toBeInstanceOf(RustProducerError);
  });

  it("passes correlation mappings through config payload", () => {
    const originalFromConfig = RustProducer.fromConfig;
    const seen: Array<Record<string, unknown>> = [];
    const stub = new StubProducer({ job_id: "job-config" });
    (RustProducer as unknown as { fromConfig: (config: Record<string, unknown>) => RustProducer })
      .fromConfig = ((config: Record<string, unknown>) => {
      seen.push(config);
      return stub as unknown as RustProducer;
    }) as (config: Record<string, unknown>) => RustProducer;

    try {
      const client = new RRQClient({
        config: {
          redisDsn: "redis://localhost:6379/0",
          correlationMappings: { session_id: "params.session.id" },
        },
      });
      void client;
    } finally {
      (
        RustProducer as unknown as { fromConfig: typeof RustProducer.fromConfig }
      ).fromConfig = originalFromConfig;
    }

    expect(seen).toHaveLength(1);
    expect(seen[0]?.correlation_mappings).toEqual({ session_id: "params.session.id" });
  });

  it("omits unique_ttl_seconds unless provided", async () => {
    const stub = new StubProducer({ job_id: "job-4" });
    const client = new RRQClient({ producer: stub as unknown as RustProducer });

    await client.enqueue("handler", { uniqueKey: "user-ttl" });

    const options = (stub.lastRequest?.options as any) ?? {};
    expect(options.unique_ttl_seconds).toBeUndefined();
  });
});
