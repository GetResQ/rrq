import { describe, expect, it } from "bun:test";

import { getProducerConstants } from "../src/producer_ffi.js";

describe("RRQ constants", () => {
  it("exposes core prefixes", () => {
    const constants = getProducerConstants() as {
      job_key_prefix: string;
      queue_key_prefix: string;
      idempotency_key_prefix: string;
    };
    expect(constants.job_key_prefix).toBeTruthy();
    expect(constants.queue_key_prefix).toBeTruthy();
    expect(constants.idempotency_key_prefix).toBeTruthy();
  });
});
