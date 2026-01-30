import { describe, expect, it } from "bun:test";

type ExecutorMessage = { type: string; payload: Record<string, unknown> };

function encodeMessage(message: ExecutorMessage): Buffer {
  const payload = Buffer.from(JSON.stringify(message));
  const header = Buffer.alloc(4);
  header.writeUInt32BE(payload.length, 0);
  return Buffer.concat([header, payload]);
}

function decodeMessage(buffer: Buffer): ExecutorMessage {
  return JSON.parse(buffer.toString("utf-8")) as ExecutorMessage;
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
