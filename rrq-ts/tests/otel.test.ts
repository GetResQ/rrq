import { afterEach, describe, expect, it } from "bun:test";
import { SpanStatusCode, context, propagation, trace } from "@opentelemetry/api";

import { OtelTelemetry } from "../src/otel.js";
import type { ExecutionRequest } from "../src/runner_runtime.js";

type StartSpanCall = {
  name: string;
  options: { kind: unknown; attributes: Record<string, unknown> };
  parentContext: unknown;
};

class FakeSpan {
  attributes: Record<string, unknown> = {};
  exceptions: unknown[] = [];
  status: unknown = null;
  ended = false;

  setAttribute(key: string, value: unknown): void {
    this.attributes[key] = value;
  }

  recordException(error: unknown): void {
    this.exceptions.push(error);
  }

  setStatus(status: unknown): void {
    this.status = status;
  }

  end(): void {
    this.ended = true;
  }
}

const originalGetTracer = trace.getTracer;
const originalExtract = propagation.extract;
const originalActive = context.active;
const originalNow = Date.now;

afterEach(() => {
  (trace as any).getTracer = originalGetTracer;
  (propagation as any).extract = originalExtract;
  (context as any).active = originalActive;
  Date.now = originalNow;
});

function buildRequest(overrides: Partial<ExecutionRequest> = {}): ExecutionRequest {
  const base: ExecutionRequest = {
    protocol_version: "2",
    request_id: "req-1",
    job_id: "job-1",
    function_name: "handler",
    params: {},
    context: {
      job_id: "job-1",
      attempt: 1,
      enqueue_time: "2024-01-01T00:00:00.000Z",
      queue_name: "default",
      deadline: null,
      trace_context: null,
      correlation_context: null,
      worker_id: null,
    },
  };
  return {
    ...base,
    ...overrides,
    context: {
      ...base.context,
      ...overrides.context,
    },
  };
}

describe("OtelTelemetry", () => {
  it("creates spans with extracted context and records outcome metadata", () => {
    const fakeSpan = new FakeSpan();
    const calls: StartSpanCall[] = [];
    const extractedParent = { extracted: "context" };

    (context as any).active = () => ({ active: true });
    (propagation as any).extract = (_ctx: unknown, _carrier: Record<string, string>) =>
      extractedParent;
    (trace as any).getTracer = () => ({
      startSpan: (
        name: string,
        options: { kind: unknown; attributes: Record<string, unknown> },
        parentContext: unknown,
      ) => {
        calls.push({ name, options, parentContext });
        return fakeSpan;
      },
    });
    Date.now = () => Date.parse("2024-01-01T00:00:10.000Z");

    const telemetry = new OtelTelemetry();
    const request = buildRequest({
      context: {
        job_id: "job-1",
        attempt: 2,
        enqueue_time: "2024-01-01T00:00:00.000Z",
        queue_name: "priority",
        deadline: "2024-01-01T00:00:20.000Z",
        trace_context: { traceparent: "00-abc-123-01" },
        correlation_context: { tenant_id: "tenant-1", empty_value: "", "": "ignored" },
        worker_id: "worker-1",
      },
    });

    const span = telemetry.runnerSpan(request);
    span.success(1.2);
    span.retry(2.5, 3, "requeue");
    span.timeout(4, 5, "timed out");
    span.error(6, new Error("boom"));
    span.cancelled(7, "manual");
    span.close();

    expect(calls).toHaveLength(1);
    expect(calls[0]?.name).toBe("rrq.runner");
    expect(calls[0]?.parentContext).toBe(extractedParent);
    const attributes = calls[0]?.options.attributes ?? {};
    expect(attributes["rrq.job_id"]).toBe("job-1");
    expect(attributes["rrq.function"]).toBe("handler");
    expect(attributes["rrq.queue"]).toBe("priority");
    expect(attributes["rrq.attempt"]).toBe(2);
    expect(attributes["rrq.worker_id"]).toBe("worker-1");
    expect(attributes["tenant_id"]).toBe("tenant-1");
    expect(attributes["empty_value"]).toBeUndefined();
    expect(attributes["rrq.queue_wait_ms"]).toBe(10000);
    expect(fakeSpan.attributes["rrq.deadline"]).toBe("2024-01-01T00:00:20.000Z");
    expect(fakeSpan.attributes["rrq.deadline_remaining_ms"]).toBe(10000);
    expect(fakeSpan.attributes["rrq.retry_delay_ms"]).toBe(3000);
    expect(fakeSpan.attributes["rrq.timeout_seconds"]).toBe(5);
    expect(fakeSpan.attributes["rrq.error_message"]).toBe("boom");
    expect(fakeSpan.attributes["rrq.error_type"]).toBe("Error");
    expect(fakeSpan.exceptions).toHaveLength(1);
    expect(fakeSpan.status).toEqual({ code: SpanStatusCode.ERROR });
    expect(fakeSpan.attributes["rrq.outcome"]).toBe("cancelled");
    expect(fakeSpan.ended).toBe(true);
  });

  it("uses active context without extraction and handles non-Error failures", () => {
    const fakeSpan = new FakeSpan();
    const calls: StartSpanCall[] = [];
    const activeContext = { active: "context" };
    let extractCalled = false;

    (context as any).active = () => activeContext;
    (propagation as any).extract = () => {
      extractCalled = true;
      return { unexpected: true };
    };
    (trace as any).getTracer = () => ({
      startSpan: (
        name: string,
        options: { kind: unknown; attributes: Record<string, unknown> },
        parentContext: unknown,
      ) => {
        calls.push({ name, options, parentContext });
        return fakeSpan;
      },
    });

    const telemetry = new OtelTelemetry();
    const request = buildRequest({
      context: {
        job_id: "job-1",
        attempt: 1,
        enqueue_time: "not-a-date",
        queue_name: "default",
        deadline: "not-a-date",
        trace_context: null,
        correlation_context: null,
        worker_id: null,
      },
    });

    const span = telemetry.runnerSpan(request);
    span.timeout(0.5, null, null);
    span.error(1, "not an error object");

    expect(extractCalled).toBe(false);
    expect(calls[0]?.parentContext).toBe(activeContext);
    const attributes = calls[0]?.options.attributes ?? {};
    expect(attributes["rrq.worker_id"]).toBeUndefined();
    expect(attributes["rrq.queue_wait_ms"]).toBeUndefined();
    expect(fakeSpan.attributes["rrq.deadline_remaining_ms"]).toBeUndefined();
    expect(fakeSpan.attributes["rrq.error_message"]).toBe("not an error object");
    expect(fakeSpan.exceptions).toHaveLength(0);
  });
});
