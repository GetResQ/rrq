import net from "node:net";
import { fileURLToPath } from "node:url";

export const ENV_RUNNER_TCP_SOCKET = "RRQ_RUNNER_TCP_SOCKET";
export const PROTOCOL_VERSION = "2";
const MAX_FRAME_LEN = 16 * 1024 * 1024;
const MAX_IN_FLIGHT_PER_CONNECTION = 64;

export type OutcomeStatus = "success" | "retry" | "timeout" | "error";

export interface RunnerSpan {
  success: (durationSeconds: number) => void;
  retry: (durationSeconds: number, delaySeconds?: number | null, reason?: string | null) => void;
  timeout: (
    durationSeconds: number,
    timeoutSeconds?: number | null,
    errorMessage?: string | null,
  ) => void;
  error: (durationSeconds: number, error?: unknown) => void;
  cancelled: (durationSeconds: number, reason?: string | null) => void;
  close: () => void;
}

export interface Telemetry {
  runnerSpan: (request: ExecutionRequest) => RunnerSpan;
}

class NoopRunnerSpan implements RunnerSpan {
  success(): void {}
  retry(): void {}
  timeout(): void {}
  error(): void {}
  cancelled(): void {}
  close(): void {}
}

export class NoopTelemetry implements Telemetry {
  private span = new NoopRunnerSpan();

  runnerSpan(): RunnerSpan {
    return this.span;
  }
}

export interface ExecutionContext {
  job_id: string;
  attempt: number;
  enqueue_time: string;
  queue_name: string;
  deadline?: string | null;
  trace_context?: Record<string, string> | null;
  worker_id?: string | null;
}

export interface ExecutionRequest {
  protocol_version: string;
  request_id: string;
  job_id: string;
  function_name: string;
  params: Record<string, unknown>;
  context: ExecutionContext;
}

export interface ExecutionError {
  message: string;
  type?: string | null;
  code?: string | null;
  details?: Record<string, unknown> | null;
}

export interface ExecutionOutcome {
  job_id: string;
  request_id: string | null;
  status: OutcomeStatus;
  result?: unknown | null;
  error?: ExecutionError | null;
  retry_after_seconds?: number | null;
}

export interface CancelRequest {
  protocol_version: string;
  job_id: string;
  request_id?: string | null;
  hard_kill?: boolean;
}

type RunnerMessage =
  | { type: "request"; payload: ExecutionRequest }
  | { type: "response"; payload: ExecutionOutcome }
  | { type: "cancel"; payload: CancelRequest };

export type Handler = (
  request: ExecutionRequest,
  signal: AbortSignal,
) => Promise<ExecutionOutcome | unknown> | ExecutionOutcome | unknown;

export class Registry {
  private handlers = new Map<string, Handler>();

  register(name: string, handler: Handler): void {
    this.handlers.set(name, handler);
  }

  async execute(request: ExecutionRequest, signal: AbortSignal): Promise<ExecutionOutcome> {
    const handler = this.handlers.get(request.function_name);
    if (!handler) {
      return errorOutcome(request, "handler_not_found", "Handler not found");
    }

    try {
      const result = await handler(request, signal);
      if (isExecutionOutcome(result)) {
        return result;
      }
      return {
        job_id: request.job_id,
        request_id: request.request_id,
        status: "success",
        result,
      };
    } catch (error) {
      if (isAbortError(error)) {
        return errorOutcome(request, "cancelled", "Job cancelled");
      }
      return errorOutcome(request, undefined, asErrorMessage(error));
    }
  }
}

interface InFlightTask {
  controller: AbortController;
  respond: (outcome: ExecutionOutcome) => Promise<void>;
  job_id: string;
  activeRequests: Set<string>;
  completed: boolean;
}

export class RunnerRuntime {
  private registry: Registry;
  private telemetry: Telemetry;
  private inFlight = new Map<string, InFlightTask>();
  private jobIndex = new Map<string, Set<string>>();

  constructor(registry: Registry, telemetry: Telemetry = new NoopTelemetry()) {
    this.registry = registry;
    this.telemetry = telemetry;
  }

  async runFromEnv(): Promise<void> {
    const tcpSocket = process.env[ENV_RUNNER_TCP_SOCKET];
    if (tcpSocket) {
      const address = parseTcpSocket(tcpSocket);
      await this.runTcp(address.host, address.port);
      return;
    }
    throw new Error(`${ENV_RUNNER_TCP_SOCKET} must be set`);
  }

  async runTcp(host: string, port: number): Promise<void> {
    await this.runServer({ host, port });
  }

  private async runServer(options: net.ListenOptions): Promise<void> {
    const server = net.createServer((socket) => this.handleConnection(socket));
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(options, resolve);
    });
  }

  private handleConnection(socket: net.Socket): void {
    const parser = new FrameParser();
    const activeRequests = new Set<string>();
    let writeChain = Promise.resolve();

    const enqueueWrite = (message: RunnerMessage): Promise<void> => {
      writeChain = writeChain
        .then(() => writeFrame(socket, message))
        .catch((error) => {
          console.error("runner response write failed:", error);
        });
      return writeChain;
    };

    const respond = async (outcome: ExecutionOutcome): Promise<void> => {
      await enqueueWrite({ type: "response", payload: outcome });
    };

    socket.on("data", (chunk) => {
      let messages: RunnerMessage[];
      try {
        messages = parser.push(chunk);
      } catch (error) {
        console.error("runner message parse error:", error);
        socket.destroy();
        return;
      }
      for (const message of messages) {
        if (message.type === "request") {
          const request = message.payload;
          if (request.protocol_version !== PROTOCOL_VERSION) {
            void respond(errorOutcome(request, undefined, "Unsupported protocol version"));
            continue;
          }

          if (activeRequests.size >= MAX_IN_FLIGHT_PER_CONNECTION) {
            void respond(
              errorOutcome(request, undefined, "Runner busy: too many in-flight requests"),
            );
            continue;
          }

          const controller = new AbortController();
          const requestId = request.request_id;
          const jobId = request.job_id;

          this.inFlight.set(requestId, {
            controller,
            respond,
            job_id: jobId,
            activeRequests,
            completed: false,
          });
          const jobEntries = this.jobIndex.get(jobId) ?? new Set<string>();
          jobEntries.add(requestId);
          this.jobIndex.set(jobId, jobEntries);
          activeRequests.add(requestId);

          void this.executeWithDeadline(request, controller)
            .then(async (outcome) => {
              const task = this.inFlight.get(requestId);
              if (!task || task.completed) {
                return;
              }
              task.completed = true;
              await respond(outcome);
              this.cleanupRequest(requestId, task.job_id, task.activeRequests);
            })
            .catch(async (error) => {
              const task = this.inFlight.get(requestId);
              if (!task || task.completed) {
                return;
              }
              task.completed = true;
              await respond(errorOutcome(request, undefined, asErrorMessage(error)));
              this.cleanupRequest(requestId, task.job_id, task.activeRequests);
            });
        } else if (message.type === "cancel") {
          this.handleCancel(message.payload);
        } else {
          void respond({
            job_id: "unknown",
            request_id: null,
            status: "error",
            error: { message: "Unexpected response message" },
          });
        }
      }
    });

    socket.on("close", () => {
      const pending = Array.from(activeRequests);
      for (const requestId of pending) {
        const task = this.inFlight.get(requestId);
        if (!task) {
          continue;
        }
        task.controller.abort();
        this.cleanupRequest(requestId, task.job_id, task.activeRequests);
      }
    });

    socket.on("error", (error) => {
      console.error("runner connection error:", error);
      socket.destroy();
    });
  }

  private async executeWithDeadline(
    request: ExecutionRequest,
    controller: AbortController,
  ): Promise<ExecutionOutcome> {
    const span = this.telemetry.runnerSpan(request);
    const start = Date.now();
    const signal = controller.signal;
    const deadline = request.context.deadline;
    if (!deadline) {
      const outcome = await this.registry.execute(request, signal);
      recordOutcome(span, outcome, (Date.now() - start) / 1000);
      span.close();
      return outcome;
    }

    const deadlineDate = new Date(deadline);
    if (Number.isNaN(deadlineDate.getTime())) {
      const outcome = errorOutcome(request, undefined, "Invalid deadline timestamp");
      recordOutcome(span, outcome, (Date.now() - start) / 1000);
      span.close();
      return outcome;
    }
    const remainingMs = deadlineDate.getTime() - Date.now();
    if (remainingMs <= 0) {
      const outcome = timeoutOutcome(request, "Job deadline exceeded");
      recordOutcome(span, outcome, (Date.now() - start) / 1000);
      span.close();
      return outcome;
    }

    try {
      const outcome = await withTimeout(
        this.registry.execute(request, signal),
        remainingMs,
        signal,
        () => controller.abort(),
      );
      recordOutcome(span, outcome, (Date.now() - start) / 1000);
      span.close();
      return outcome;
    } catch (error) {
      let outcome: ExecutionOutcome;
      if (isAbortError(error)) {
        outcome = errorOutcome(request, "cancelled", "Job cancelled");
      } else if (error instanceof TimeoutError) {
        outcome = timeoutOutcome(request, "Job execution timed out");
      } else {
        outcome = errorOutcome(request, undefined, asErrorMessage(error));
      }
      recordOutcome(span, outcome, (Date.now() - start) / 1000, error);
      span.close();
      return outcome;
    }
  }

  private handleCancel(payload: CancelRequest): void {
    if (payload.protocol_version !== PROTOCOL_VERSION) {
      return;
    }
    const requestIds = payload.request_id
      ? [payload.request_id]
      : Array.from(this.jobIndex.get(payload.job_id) ?? []);
    if (requestIds.length === 0) {
      return;
    }
    for (const requestId of requestIds) {
      const task = this.inFlight.get(requestId);
      if (!task || task.completed) {
        continue;
      }
      task.completed = true;
      task.controller.abort();
      void task
        .respond({
          job_id: payload.job_id,
          request_id: requestId,
          status: "error",
          error: { message: "Job cancelled", type: "cancelled" },
        })
        .finally(() => {
          this.cleanupRequest(requestId, task.job_id, task.activeRequests);
        });
    }
  }

  private cleanupRequest(requestId: string, jobId: string, activeRequests: Set<string>): void {
    this.inFlight.delete(requestId);
    const jobEntries = this.jobIndex.get(jobId);
    if (jobEntries) {
      jobEntries.delete(requestId);
      if (jobEntries.size === 0) {
        this.jobIndex.delete(jobId);
      }
    }
    activeRequests.delete(requestId);
  }
}

export function parseTcpSocket(value: string): { host: string; port: number } {
  const raw = value.trim();
  if (!raw) {
    throw new Error("runner tcp_socket cannot be empty");
  }

  let host: string;
  let portPart: string;
  if (raw.startsWith("[")) {
    const closing = raw.indexOf("]:");
    if (closing === -1) {
      throw new Error("runner tcp_socket must be in [host]:port format");
    }
    host = raw.slice(1, closing);
    portPart = raw.slice(closing + 2);
  } else {
    const lastColon = raw.lastIndexOf(":");
    if (lastColon === -1) {
      throw new Error("runner tcp_socket must be in host:port format");
    }
    host = raw.slice(0, lastColon);
    portPart = raw.slice(lastColon + 1);
    if (!host) {
      throw new Error("runner tcp_socket host cannot be empty");
    }
  }

  const port = Number.parseInt(portPart, 10);
  if (!Number.isFinite(port) || port <= 0 || port > 65535) {
    throw new Error(`Invalid runner tcp_socket port: ${portPart}`);
  }

  if (host === "localhost") {
    return { host: "127.0.0.1", port };
  }
  if (!net.isIP(host)) {
    throw new Error(`Invalid runner tcp_socket host: ${host}`);
  }
  if (!isLoopback(host)) {
    throw new Error("runner tcp_socket host must be localhost");
  }
  return { host, port };
}

class FrameParser {
  private buffer = Buffer.alloc(0);

  push(chunk: Buffer): RunnerMessage[] {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    const messages: RunnerMessage[] = [];
    while (this.buffer.length >= 4) {
      const length = this.buffer.readUInt32BE(0);
      if (length === 0) {
        throw new Error("runner message payload cannot be empty");
      }
      if (length > MAX_FRAME_LEN) {
        throw new Error("runner message payload too large");
      }
      if (this.buffer.length < 4 + length) {
        break;
      }
      const payload = this.buffer.subarray(4, 4 + length);
      this.buffer = this.buffer.subarray(4 + length);
      const decoded = JSON.parse(payload.toString("utf-8")) as RunnerMessage;
      messages.push(decoded);
    }
    return messages;
  }
}

async function writeFrame(socket: net.Socket, message: RunnerMessage): Promise<void> {
  const payload = Buffer.from(JSON.stringify(message));
  const header = Buffer.alloc(4);
  header.writeUInt32BE(payload.length, 0);
  await new Promise<void>((resolve, reject) => {
    socket.write(Buffer.concat([header, payload]), (error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}

function errorOutcome(
  request: ExecutionRequest,
  type: string | undefined,
  message: string,
): ExecutionOutcome {
  return {
    job_id: request.job_id,
    request_id: request.request_id,
    status: "error",
    error: {
      message,
      type: type ?? null,
    },
  };
}

function timeoutOutcome(request: ExecutionRequest, message: string): ExecutionOutcome {
  return {
    job_id: request.job_id,
    request_id: request.request_id,
    status: "timeout",
    error: {
      message,
      type: "timeout",
    },
  };
}

function recordOutcome(
  span: RunnerSpan,
  outcome: ExecutionOutcome,
  durationSeconds: number,
  error?: unknown,
): void {
  switch (outcome.status) {
    case "success":
      span.success(durationSeconds);
      return;
    case "retry":
      span.retry(
        durationSeconds,
        outcome.retry_after_seconds ?? undefined,
        outcome.error?.message ?? undefined,
      );
      return;
    case "timeout":
      span.timeout(durationSeconds, undefined, outcome.error?.message ?? undefined);
      return;
    case "error":
      if (outcome.error?.type === "cancelled") {
        span.cancelled(durationSeconds, outcome.error?.message ?? undefined);
        return;
      }
      span.error(durationSeconds, error ?? outcome.error?.message);
      return;
    default:
      return;
  }
}

function isExecutionOutcome(value: unknown): value is ExecutionOutcome {
  if (!value || typeof value !== "object") {
    return false;
  }
  const candidate = value as ExecutionOutcome;
  return (
    typeof candidate.status === "string" &&
    typeof candidate.job_id === "string" &&
    "request_id" in candidate
  );
}

function asErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

function isAbortError(error: unknown): boolean {
  if (!error) {
    return false;
  }
  return (
    error instanceof Error && (error.name === "AbortError" || error.message === "Job cancelled")
  );
}

class TimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TimeoutError";
  }
}

function createAbortError(): Error {
  if (typeof globalThis.DOMException === "function") {
    return new globalThis.DOMException("Job cancelled", "AbortError");
  }
  const fallback = new Error("Job cancelled");
  fallback.name = "AbortError";
  return fallback;
}

function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  signal: AbortSignal,
  onTimeout?: () => void,
): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  return new Promise<T>((resolve, reject) => {
    if (signal.aborted) {
      reject(createAbortError());
      return;
    }

    const onAbort = () => {
      cleanup();
      reject(createAbortError());
    };

    const cleanup = () => {
      if (timer) {
        clearTimeout(timer);
      }
      signal.removeEventListener("abort", onAbort);
    };

    signal.addEventListener("abort", onAbort);
    timer = setTimeout(() => {
      cleanup();
      if (onTimeout) {
        onTimeout();
      }
      reject(new TimeoutError("Job execution timed out"));
    }, timeoutMs);

    promise
      .then((value) => {
        cleanup();
        resolve(value);
      })
      .catch((error) => {
        cleanup();
        reject(error);
      });
  });
}

function isLoopback(host: string): boolean {
  if (net.isIP(host) === 4) {
    return host.startsWith("127.");
  }
  return host === "::1";
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const registry = new Registry();
  registry.register("echo", (request) => ({
    job_id: request.job_id,
    request_id: request.request_id,
    status: "success",
    result: { job_id: request.job_id },
  }));

  const runtime = new RunnerRuntime(registry);
  runtime
    .runFromEnv()
    .catch((error) => {
      console.error(error);
      process.exitCode = 1;
    })
    .finally(() => {
      process.exitCode = process.exitCode ?? 0;
    });
}
