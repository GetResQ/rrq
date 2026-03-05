import { describe, expect, it } from "bun:test";

import * as rrq from "../src/index.js";
import { OtelTelemetry } from "../src/otel.js";
import { RRQClient } from "../src/producer.js";
import { Registry, RunnerRuntime, parseTcpSocket } from "../src/runner_runtime.js";

describe("index exports", () => {
  it("re-exports producer, runner, and telemetry APIs", () => {
    expect(rrq.RRQClient).toBe(RRQClient);
    expect(rrq.Registry).toBe(Registry);
    expect(rrq.RunnerRuntime).toBe(RunnerRuntime);
    expect(rrq.parseTcpSocket).toBe(parseTcpSocket);
    expect(rrq.OtelTelemetry).toBe(OtelTelemetry);
  });
});
