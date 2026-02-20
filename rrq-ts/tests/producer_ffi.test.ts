import { describe, expect, it } from "bun:test";

import { RustProducer } from "../src/producer_ffi.js";

type RustProducerPrivates = {
  callNodeFunctionAsync: <TArgs extends unknown[], TResult>(
    fn: ((...args: TArgs) => TResult) & {
      async?: (...args: [...TArgs, (err: unknown, result: TResult) => void]) => void;
    },
    ...args: TArgs
  ) => Promise<TResult>;
};

const rustProducerPrivates = RustProducer as unknown as RustProducerPrivates;

describe("RustProducer node async helper", () => {
  it("uses async koffi calls when available", async () => {
    let syncCallCount = 0;
    const ffiFn = ((_: string) => {
      syncCallCount += 1;
      return "sync-result";
    }) as ((input: string) => string) & {
      async: (input: string, cb: (err: unknown, result: string) => void) => void;
    };
    ffiFn.async = (_input: string, cb: (err: unknown, result: string) => void) => {
      setTimeout(() => cb(null, "async-result"), 0);
    };

    const resultPromise = rustProducerPrivates.callNodeFunctionAsync(ffiFn, "payload");
    await Promise.resolve();

    expect(syncCallCount).toBe(0);
    await expect(resultPromise).resolves.toBe("async-result");
  });

  it("falls back to sync invocation when async is unavailable", async () => {
    let syncCallCount = 0;
    const ffiFn = (() => {
      syncCallCount += 1;
      return "sync-result";
    }) as () => string;

    await expect(rustProducerPrivates.callNodeFunctionAsync(ffiFn)).resolves.toBe("sync-result");
    expect(syncCallCount).toBe(1);
  });
});
