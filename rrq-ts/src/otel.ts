import {
  type Attributes,
  SpanKind,
  SpanStatusCode,
  context,
  propagation,
  trace,
} from "@opentelemetry/api";

import type { ExecutionRequest, RunnerSpan, Telemetry } from "./runner_runtime.js";

class OtelRunnerSpan implements RunnerSpan {
  private span: any;

  constructor(span: any) {
    this.span = span;
  }

  success(durationSeconds: number): void {
    this.span.setAttribute("rrq.outcome", "success");
    this.span.setAttribute("rrq.duration_ms", durationSeconds * 1000);
  }

  retry(durationSeconds: number, delaySeconds?: number | null, reason?: string | null): void {
    this.span.setAttribute("rrq.outcome", "retry");
    this.span.setAttribute("rrq.duration_ms", durationSeconds * 1000);
    if (delaySeconds !== undefined && delaySeconds !== null) {
      this.span.setAttribute("rrq.retry_delay_ms", delaySeconds * 1000);
    }
    if (reason) {
      this.span.setAttribute("rrq.reason", reason);
    }
  }

  timeout(
    durationSeconds: number,
    timeoutSeconds?: number | null,
    errorMessage?: string | null,
  ): void {
    this.span.setAttribute("rrq.outcome", "timeout");
    this.span.setAttribute("rrq.duration_ms", durationSeconds * 1000);
    if (timeoutSeconds !== undefined && timeoutSeconds !== null) {
      this.span.setAttribute("rrq.timeout_seconds", timeoutSeconds);
    }
    if (errorMessage) {
      this.span.setAttribute("rrq.error_message", errorMessage);
    }
  }

  error(durationSeconds: number, error?: unknown): void {
    this.span.setAttribute("rrq.outcome", "error");
    this.span.setAttribute("rrq.duration_ms", durationSeconds * 1000);
    if (error instanceof Error) {
      this.span.recordException(error);
      this.span.setStatus({ code: SpanStatusCode.ERROR });
      this.span.setAttribute("rrq.error_message", error.message);
      this.span.setAttribute("rrq.error_type", error.name);
    } else if (error) {
      this.span.setAttribute("rrq.error_message", String(error));
    }
  }

  cancelled(durationSeconds: number, reason?: string | null): void {
    this.span.setAttribute("rrq.outcome", "cancelled");
    this.span.setAttribute("rrq.duration_ms", durationSeconds * 1000);
    if (reason) {
      this.span.setAttribute("rrq.reason", reason);
    }
  }

  close(): void {
    this.span.end();
  }
}

export class OtelTelemetry implements Telemetry {
  runnerSpan(request: ExecutionRequest): RunnerSpan {
    const tracer = trace.getTracer("rrq");
    const carrier = request.context.trace_context ?? undefined;
    const parentContext = carrier
      ? propagation.extract(context.active(), carrier)
      : context.active();
    const attributes: Attributes = {
      "rrq.job_id": request.job_id,
      "rrq.function": request.function_name,
      "rrq.queue": request.context.queue_name,
      "rrq.attempt": request.context.attempt,
      "span.kind": "consumer",
      "messaging.system": "redis",
      "messaging.destination.name": request.context.queue_name,
      "messaging.destination_kind": "queue",
      "messaging.operation": "process",
    };
    if (request.context.worker_id) {
      attributes["rrq.worker_id"] = request.context.worker_id;
    }
    const correlationContext = request.context.correlation_context;
    if (correlationContext) {
      for (const [key, value] of Object.entries(correlationContext)) {
        if (!key || !value) {
          continue;
        }
        attributes[key] = value;
      }
    }
    const enqueueMs = Date.parse(request.context.enqueue_time);
    if (!Number.isNaN(enqueueMs)) {
      attributes["rrq.queue_wait_ms"] = Math.max(Date.now() - enqueueMs, 0);
    }
    const span = tracer.startSpan(
      "rrq.runner",
      {
        kind: SpanKind.CONSUMER,
        attributes,
      },
      parentContext,
    );
    if (request.context.deadline) {
      span.setAttribute("rrq.deadline", request.context.deadline);
      const deadlineMs = Date.parse(request.context.deadline);
      if (!Number.isNaN(deadlineMs)) {
        span.setAttribute("rrq.deadline_remaining_ms", Math.max(deadlineMs - Date.now(), 0));
      }
    }
    return new OtelRunnerSpan(span);
  }
}
