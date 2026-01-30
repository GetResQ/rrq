import { describe, expect, it } from "bun:test";

import { getProducerConstants } from "../src/producer_ffi.js";

describe("RRQ constants", () => {
  it("exposes defaults aligned with key prefixes", () => {
    const constants = getProducerConstants();
    expect(constants.default_queue_name).toBe(
      `${constants.queue_key_prefix}default`,
    );
  });

  it("exposes core prefixes and default values", () => {
    const constants = getProducerConstants();
    expect(constants.job_key_prefix).toBeTruthy();
    expect(constants.queue_key_prefix).toBeTruthy();
    expect(constants.idempotency_key_prefix).toBeTruthy();

    expect(constants.default_max_retries).toBeGreaterThan(0);
    expect(constants.default_job_timeout_seconds).toBeGreaterThan(0);
    expect(constants.default_result_ttl_seconds).toBeGreaterThan(0);
    expect(constants.default_unique_job_lock_ttl_seconds).toBeGreaterThan(0);
  });
});
