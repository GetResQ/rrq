import { describe, expect, it } from "bun:test";

type RunnerMessage = { type: string; payload: Record<string, unknown> };

function encodeMessage(message: RunnerMessage): Buffer {
  const payload = Buffer.from(JSON.stringify(message));
  const header = Buffer.alloc(4);
  header.writeUInt32BE(payload.length, 0);
  return Buffer.concat([header, payload]);
}

function decodeMessage(buffer: Buffer): RunnerMessage {
  return JSON.parse(buffer.toString("utf-8")) as RunnerMessage;
}

describe("protocol framing", () => {
  it("encodes and decodes a message roundtrip", () => {
    const payload = { job_id: "job-1", status: "success" };
    const frame = encodeMessage({ type: "response", payload });
    const length = frame.readUInt32BE(0);
    expect(length).toBe(frame.length - 4);
    const decoded = decodeMessage(frame.subarray(4));
    expect(decoded.type).toBe("response");
    expect(decoded.payload).toEqual(payload);
  });
});
