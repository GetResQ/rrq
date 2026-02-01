import net from "node:net";

import { describe, expect, it } from "bun:test";

import { RunnerRuntime, Registry } from "../src/runner_runtime.js";

type RunnerMessage =
  | { type: "request"; payload: Record<string, unknown> }
  | { type: "response"; payload: Record<string, unknown> }
  | { type: "cancel"; payload: Record<string, unknown> };

type ExecutionRequestPayload = {
  protocol_version: string;
  request_id: string;
  job_id: string;
  function_name: string;
  params: Record<string, unknown>;
  context: {
    job_id: string;
    attempt: number;
    enqueue_time: string;
    queue_name: string;
    deadline: string | null;
    trace_context: Record<string, string> | null;
    worker_id: string | null;
  };
};

function encodeMessage(message: RunnerMessage): Buffer {
  const payload = Buffer.from(JSON.stringify(message));
  const header = Buffer.alloc(4);
  header.writeUInt32BE(payload.length, 0);
  return Buffer.concat([header, payload]);
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
      if (this.buffer.length < 4 + length) {
        break;
      }
      const payload = this.buffer.subarray(4, 4 + length);
      this.buffer = this.buffer.subarray(4 + length);
      messages.push(JSON.parse(payload.toString("utf-8")) as RunnerMessage);
    }
    return messages;
  }
}

async function waitForMessages(socket: net.Socket, count: number): Promise<RunnerMessage[]> {
  const parser = new FrameParser();
  const messages: RunnerMessage[] = [];
  return await new Promise((resolve, reject) => {
    const onData = (chunk: Buffer) => {
      try {
        messages.push(...parser.push(chunk));
        if (messages.length >= count) {
          cleanup();
          resolve(messages.slice(0, count));
        }
      } catch (error) {
        cleanup();
        reject(error);
      }
    };
    const onError = (error: Error) => {
      cleanup();
      reject(error);
    };
    const cleanup = () => {
      socket.off("data", onData);
      socket.off("error", onError);
    };
    socket.on("data", onData);
    socket.on("error", onError);
  });
}

async function withServer(
  registry: Registry,
  handler: (socket: net.Socket) => void,
): Promise<{ server: net.Server; port: number }> {
  const server = net.createServer((socket) => handler(socket));
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("failed to bind server");
  }
  return { server, port: address.port };
}

function buildRequest(overrides?: Partial<ExecutionRequestPayload>): ExecutionRequestPayload {
  return {
    protocol_version: "2",
    request_id: "req-1",
    job_id: "job-1",
    function_name: "handler",
    params: {},
    context: {
      job_id: "job-1",
      attempt: 1,
      enqueue_time: new Date().toISOString(),
      queue_name: "default",
      deadline: null,
      trace_context: null,
      worker_id: null,
    },
    ...overrides,
  };
}

describe("RunnerRuntime integration", () => {
  it("executes request with future deadline", async () => {
    const registry = new Registry();
    registry.register("handler", async () => ({ ok: true }));
    const runtime = new RunnerRuntime(registry);
    const { server, port } = await withServer(registry, (socket) =>
      (runtime as any).handleConnection(socket),
    );

    const client = net.connect(port, "127.0.0.1");
    try {
      const deadline = new Date(Date.now() + 1000).toISOString();
      const base = buildRequest();
      const request = buildRequest({
        context: { ...base.context, deadline },
      });
      client.write(encodeMessage({ type: "request", payload: request }));
      const [message] = await waitForMessages(client, 1);
      expect(message.type).toBe("response");
      expect(message.payload.status).toBe("success");
    } finally {
      client.end();
      await new Promise((resolve) => client.once("close", resolve));
      await new Promise<void>((resolve) => server.close(resolve));
    }
  });

  it("returns timeout when deadline is in the past", async () => {
    const registry = new Registry();
    registry.register("handler", async () => ({ ok: true }));
    const runtime = new RunnerRuntime(registry);
    const { server, port } = await withServer(registry, (socket) =>
      (runtime as any).handleConnection(socket),
    );

    const client = net.connect(port, "127.0.0.1");
    try {
      const deadline = new Date(Date.now() - 1000).toISOString();
      const base = buildRequest();
      const request = buildRequest({
        context: { ...base.context, deadline },
      });
      client.write(encodeMessage({ type: "request", payload: request }));
      const [message] = await waitForMessages(client, 1);
      expect(message.type).toBe("response");
      expect(message.payload.status).toBe("timeout");
      expect(message.payload.error?.type).toBe("timeout");
    } finally {
      client.end();
      await new Promise((resolve) => client.once("close", resolve));
      await new Promise<void>((resolve) => server.close(resolve));
    }
  });

  it("cancels all requests for a job id", async () => {
    const registry = new Registry();
    registry.register("handler", async (_request, signal) => {
      await new Promise((_resolve, reject) => {
        signal.addEventListener("abort", () => {
          const err = new Error("Job cancelled");
          err.name = "AbortError";
          reject(err);
        });
      });
      return { ok: true };
    });
    const runtime = new RunnerRuntime(registry);
    const { server, port } = await withServer(registry, (socket) =>
      (runtime as any).handleConnection(socket),
    );

    const client = net.connect(port, "127.0.0.1");
    try {
      const jobId = "job-cancel";
      const req1 = buildRequest({ request_id: "req-1", job_id: jobId });
      const req2 = buildRequest({ request_id: "req-2", job_id: jobId });
      client.write(encodeMessage({ type: "request", payload: req1 }));
      client.write(encodeMessage({ type: "request", payload: req2 }));

      await new Promise((resolve) => setTimeout(resolve, 10));

      client.write(
        encodeMessage({
          type: "cancel",
          payload: {
            protocol_version: "2",
            job_id: jobId,
            request_id: null,
            hard_kill: false,
          },
        }),
      );

      const responses = await waitForMessages(client, 2);
      for (const message of responses) {
        expect(message.type).toBe("response");
        expect(message.payload.job_id).toBe(jobId);
        expect(message.payload.status).toBe("error");
        expect(message.payload.error?.type).toBe("cancelled");
      }
    } finally {
      client.end();
      await new Promise((resolve) => client.once("close", resolve));
      await new Promise<void>((resolve) => server.close(resolve));
    }
  });

  it("returns busy when in-flight limit is exceeded", async () => {
    const registry = new Registry();
    registry.register("handler", async (_request, signal) => {
      await new Promise((_resolve, reject) => {
        signal.addEventListener("abort", () => {
          const err = new Error("Job cancelled");
          err.name = "AbortError";
          reject(err);
        });
      });
      return { ok: true };
    });
    const runtime = new RunnerRuntime(registry);
    const { server, port } = await withServer(registry, (socket) =>
      (runtime as any).handleConnection(socket),
    );

    const client = net.connect(port, "127.0.0.1");
    try {
      const jobId = "job-busy";
      const maxInFlight = 64;
      for (let i = 0; i < maxInFlight; i += 1) {
        client.write(
          encodeMessage({
            type: "request",
            payload: buildRequest({ request_id: `req-${i}`, job_id: jobId }),
          }),
        );
      }
      client.write(
        encodeMessage({
          type: "request",
          payload: buildRequest({ request_id: "req-busy", job_id: jobId }),
        }),
      );

      const [busyResponse] = await waitForMessages(client, 1);
      expect(busyResponse.type).toBe("response");
      expect(busyResponse.payload.request_id).toBe("req-busy");
      expect(busyResponse.payload.status).toBe("error");
      expect(busyResponse.payload.error?.message).toMatch(/Runner busy/);

      client.write(
        encodeMessage({
          type: "cancel",
          payload: {
            protocol_version: "2",
            job_id: jobId,
            request_id: null,
            hard_kill: false,
          },
        }),
      );

      const responses = await waitForMessages(client, maxInFlight);
      for (const message of responses) {
        expect(message.type).toBe("response");
        expect(message.payload.status).toBe("error");
        expect(message.payload.error?.type).toBe("cancelled");
      }
    } finally {
      client.end();
      await new Promise((resolve) => client.once("close", resolve));
      await new Promise<void>((resolve) => server.close(resolve));
    }
  });
});
