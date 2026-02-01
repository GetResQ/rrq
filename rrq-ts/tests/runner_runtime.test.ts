import { describe, expect, it } from "bun:test";

import { Registry, parseTcpSocket } from "../src/runner_runtime.js";

const baseRequest = {
  protocol_version: "2",
  request_id: "req-1",
  job_id: "job-1",
  function_name: "handler",
  params: {},
  context: {
    job_id: "job-1",
    attempt: 1,
    enqueue_time: "2024-01-01T00:00:00Z",
    queue_name: "default",
    deadline: null,
    trace_context: null,
    worker_id: null,
  },
};

describe("Registry", () => {
  it("returns handler_not_found when no handler is registered", async () => {
    const registry = new Registry();
    const outcome = await registry.execute(baseRequest, new AbortController().signal);
    expect(outcome.status).toBe("error");
    expect(outcome.error?.type).toBe("handler_not_found");
  });

  it("wraps raw handler results in a success outcome", async () => {
    const registry = new Registry();
    registry.register("handler", async () => ({ ok: true }));
    const outcome = await registry.execute(baseRequest, new AbortController().signal);
    expect(outcome.status).toBe("success");
    expect(outcome.result).toEqual({ ok: true });
  });
});

describe("parseTcpSocket", () => {
  it("normalizes localhost and validates loopback only", () => {
    expect(parseTcpSocket("localhost:5555")).toEqual({
      host: "127.0.0.1",
      port: 5555,
    });
    expect(() => parseTcpSocket("0.0.0.0:5555")).toThrow(
      "runner tcp_socket host must be localhost",
    );
  });

  it("rejects invalid ports", () => {
    expect(() => parseTcpSocket("localhost:0")).toThrow("Invalid runner tcp_socket port");
    expect(() => parseTcpSocket("localhost:99999")).toThrow("Invalid runner tcp_socket port");
  });
});
