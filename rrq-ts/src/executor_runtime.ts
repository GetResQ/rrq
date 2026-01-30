import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const ENV_EXECUTOR_SOCKET = "RRQ_EXECUTOR_SOCKET";
export const ENV_EXECUTOR_TCP_SOCKET = "RRQ_EXECUTOR_TCP_SOCKET";
export const PROTOCOL_VERSION = "1";
const MAX_FRAME_LEN = 16 * 1024 * 1024;

export type OutcomeStatus = "success" | "retry" | "timeout" | "error";

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
  args: unknown[];
  kwargs: Record<string, unknown>;
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

type ExecutorMessage =
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

  async execute(
    request: ExecutionRequest,
    signal: AbortSignal,
  ): Promise<ExecutionOutcome> {
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
}

export class ExecutorRuntime {
  private registry: Registry;
  private inFlight = new Map<string, InFlightTask>();
  private jobIndex = new Map<string, string>();

  constructor(registry: Registry) {
    this.registry = registry;
  }

  async runFromEnv(): Promise<void> {
    const socketPath = process.env[ENV_EXECUTOR_SOCKET];
    const tcpSocket = process.env[ENV_EXECUTOR_TCP_SOCKET];
    if (socketPath && tcpSocket) {
      throw new Error("Provide only one of RRQ_EXECUTOR_SOCKET or RRQ_EXECUTOR_TCP_SOCKET");
    }
    if (tcpSocket) {
      const address = parseTcpSocket(tcpSocket);
      await this.runTcp(address.host, address.port);
      return;
    }
    if (!socketPath) {
      throw new Error(
        `${ENV_EXECUTOR_SOCKET} or ${ENV_EXECUTOR_TCP_SOCKET} must be set`,
      );
    }
    await this.runSocket(socketPath);
  }

  async runSocket(socketPath: string): Promise<void> {
    await fs.promises.mkdir(path.dirname(socketPath), { recursive: true });
    if (fs.existsSync(socketPath)) {
      await fs.promises.unlink(socketPath);
    }
    await this.runServer({ path: socketPath });
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

    const enqueueWrite = (message: ExecutorMessage): Promise<void> => {
      writeChain = writeChain.then(() => writeFrame(socket, message));
      return writeChain;
    };

    const respond = async (outcome: ExecutionOutcome): Promise<void> => {
      await enqueueWrite({ type: "response", payload: outcome });
    };

    const cleanupRequest = (requestId: string, jobId: string): void => {
      this.inFlight.delete(requestId);
      this.jobIndex.delete(jobId);
      activeRequests.delete(requestId);
    };

    socket.on("data", (chunk) => {
      const messages = parser.push(chunk);
      for (const message of messages) {
        if (message.type === "request") {
          const request = message.payload;
          if (request.protocol_version !== PROTOCOL_VERSION) {
            void respond(
              errorOutcome(
                request,
                "protocol",
                "Unsupported protocol version",
              ),
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
          });
          this.jobIndex.set(jobId, requestId);
          activeRequests.add(requestId);

          void this.executeWithDeadline(request, controller.signal)
            .then(async (outcome) => {
              await respond(outcome);
              cleanupRequest(requestId, jobId);
            })
            .catch(async (error) => {
              await respond(errorOutcome(request, undefined, asErrorMessage(error)));
              cleanupRequest(requestId, jobId);
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
      for (const requestId of activeRequests) {
        const task = this.inFlight.get(requestId);
        if (task) {
          task.controller.abort();
          this.jobIndex.delete(task.job_id);
          this.inFlight.delete(requestId);
        }
      }
    });

    socket.on("error", (error) => {
      console.error("executor connection error:", error);
      socket.destroy();
    });
  }

  private async executeWithDeadline(
    request: ExecutionRequest,
    signal: AbortSignal,
  ): Promise<ExecutionOutcome> {
    const deadline = request.context.deadline;
    if (!deadline) {
      return this.registry.execute(request, signal);
    }

    const deadlineDate = new Date(deadline);
    if (Number.isNaN(deadlineDate.getTime())) {
      return errorOutcome(request, "deadline", "Invalid deadline timestamp");
    }
    const remainingMs = deadlineDate.getTime() - Date.now();
    if (remainingMs <= 0) {
      return timeoutOutcome(request, "Job deadline exceeded");
    }

    try {
      return await withTimeout(
        this.registry.execute(request, signal),
        remainingMs,
        signal,
      );
    } catch (error) {
      if (isAbortError(error)) {
        return errorOutcome(request, "cancelled", "Job cancelled");
      }
      if (error instanceof TimeoutError) {
        return timeoutOutcome(request, "Job execution timed out");
      }
      return errorOutcome(request, undefined, asErrorMessage(error));
    }
  }

  private handleCancel(payload: CancelRequest): void {
    if (payload.protocol_version !== PROTOCOL_VERSION) {
      return;
    }
    const requestId = payload.request_id ?? this.jobIndex.get(payload.job_id);
    if (!requestId) {
      return;
    }
    const task = this.inFlight.get(requestId);
    if (!task) {
      return;
    }
    task.controller.abort();
    void task.respond({
      job_id: payload.job_id,
      request_id: requestId,
      status: "error",
      error: { message: "Job cancelled", type: "cancelled" },
    });
    this.inFlight.delete(requestId);
    this.jobIndex.delete(task.job_id);
  }
}

export function parseTcpSocket(value: string): { host: string; port: number } {
  const raw = value.trim();
  if (!raw) {
    throw new Error("executor tcp_socket cannot be empty");
  }

  let host: string;
  let portPart: string;
  if (raw.startsWith("[")) {
    const closing = raw.indexOf("]:");
    if (closing === -1) {
      throw new Error("executor tcp_socket must be in [host]:port format");
    }
    host = raw.slice(1, closing);
    portPart = raw.slice(closing + 2);
  } else {
    const lastColon = raw.lastIndexOf(":");
    if (lastColon === -1) {
      throw new Error("executor tcp_socket must be in host:port format");
    }
    host = raw.slice(0, lastColon);
    portPart = raw.slice(lastColon + 1);
    if (!host) {
      throw new Error("executor tcp_socket host cannot be empty");
    }
  }

  const port = Number.parseInt(portPart, 10);
  if (!Number.isFinite(port) || port <= 0 || port > 65535) {
    throw new Error(`Invalid executor tcp_socket port: ${portPart}`);
  }

  if (host === "localhost") {
    return { host: "127.0.0.1", port };
  }
  if (!net.isIP(host)) {
    throw new Error(`Invalid executor tcp_socket host: ${host}`);
  }
  if (!isLoopback(host)) {
    throw new Error("executor tcp_socket host must be localhost");
  }
  return { host, port };
}

class FrameParser {
  private buffer = Buffer.alloc(0);

  push(chunk: Buffer): ExecutorMessage[] {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    const messages: ExecutorMessage[] = [];
    while (this.buffer.length >= 4) {
      const length = this.buffer.readUInt32BE(0);
      if (length === 0) {
        throw new Error("executor message payload cannot be empty");
      }
      if (length > MAX_FRAME_LEN) {
        throw new Error("executor message payload too large");
      }
      if (this.buffer.length < 4 + length) {
        break;
      }
      const payload = this.buffer.subarray(4, 4 + length);
      this.buffer = this.buffer.subarray(4 + length);
      const decoded = JSON.parse(payload.toString("utf-8")) as ExecutorMessage;
      messages.push(decoded);
    }
    return messages;
  }
}

async function writeFrame(
  socket: net.Socket,
  message: ExecutorMessage,
): Promise<void> {
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
    error instanceof Error &&
    (error.name === "AbortError" || error.message === "Job cancelled")
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

  const runtime = new ExecutorRuntime(registry);
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
