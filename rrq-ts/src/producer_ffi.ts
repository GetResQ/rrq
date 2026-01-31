import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

export class RustProducerError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "RustProducerError";
  }
}

let loadedLib: NodeLibrary | null = null;
let loadedKoffi: any | null = null;
let loadedBun: BunLibrary | null = null;

type BunLibrary = {
  symbols: {
    rrq_producer_new: (config: unknown, errorOut: number) => number;
    rrq_producer_new_from_toml: (path: number, errorOut: number) => number;
    rrq_producer_free: (handle: number) => void;
    rrq_producer_constants: (errorOut: number) => number;
    rrq_producer_config_from_toml: (path: number, errorOut: number) => number;
    rrq_producer_enqueue: (handle: number, request: unknown, errorOut: number) => number;
    rrq_producer_get_job_status: (handle: number, request: unknown, errorOut: number) => number;
    rrq_string_free: (ptr: number) => void;
  };
  CString: new (input: string | number) => { toString(): string };
  ptr: (value: ArrayBufferView) => number;
};

type NodeLibrary = {
  rrq_producer_new: (config: string, errorOut: Array<unknown>) => unknown;
  rrq_producer_new_from_toml: (path: string | null, errorOut: Array<unknown>) => unknown;
  rrq_producer_free: (handle: unknown) => void;
  rrq_producer_constants: (errorOut: Array<unknown>) => unknown;
  rrq_producer_config_from_toml: (path: string | null, errorOut: Array<unknown>) => unknown;
  rrq_producer_enqueue: (handle: unknown, request: string, errorOut: Array<unknown>) => unknown;
  rrq_producer_get_job_status: (
    handle: unknown,
    request: string,
    errorOut: Array<unknown>,
  ) => unknown;
  rrq_string_free: (ptr: unknown) => void;
  decode: (ptr: unknown, type: string) => string;
};

function platformLibraryName(): string | null {
  switch (process.platform) {
    case "linux":
      return "librrq_producer.so";
    case "darwin":
      return "librrq_producer.dylib";
    case "win32":
      return "rrq_producer.dll";
    default:
      return null;
  }
}

function findLibraryPath(): string {
  const override = process.env.RRQ_PRODUCER_LIB_PATH;
  if (override) {
    if (fs.existsSync(override)) {
      return override;
    }
    throw new RustProducerError(`RRQ producer library not found at ${override}`);
  }

  const baseDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "bin");
  const candidates: string[] = [];
  const platformLib = platformLibraryName();
  if (platformLib) {
    const platformDir = path.join(baseDir, `${process.platform}-${process.arch}`);
    candidates.push(path.join(platformDir, platformLib));
    candidates.push(path.join(baseDir, platformLib));
  }
  candidates.push(
    path.join(baseDir, "librrq_producer.so"),
    path.join(baseDir, "librrq_producer.dylib"),
    path.join(baseDir, "rrq_producer.dll"),
  );
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }

  throw new RustProducerError(
    "RRQ producer library not found. Set RRQ_PRODUCER_LIB_PATH or place the shared library in rrq-ts/bin.",
  );
}

function loadKoffi(): any {
  const require = createRequire(import.meta.url);
  try {
    return require("koffi");
  } catch {
    throw new RustProducerError("Failed to load koffi; ensure Node.js is available for FFI calls.");
  }
}

function getKoffi(): any {
  if (!loadedKoffi) {
    loadedKoffi = loadKoffi();
  }
  return loadedKoffi;
}

function loadNodeLibrary(): NodeLibrary {
  const koffi = getKoffi();
  const libPath = findLibraryPath();
  const lib = koffi.load(libPath);
  const rrq_producer_new = lib.func(
    "void * rrq_producer_new(const char *config_json, _Out_ void **error_out)",
  );
  const rrq_producer_new_from_toml = lib.func(
    "void * rrq_producer_new_from_toml(const char *config_path, _Out_ void **error_out)",
  );
  const rrq_producer_free = lib.func("void rrq_producer_free(void *handle)");
  const rrq_producer_constants = lib.func("void * rrq_producer_constants(_Out_ void **error_out)");
  const rrq_producer_config_from_toml = lib.func(
    "void * rrq_producer_config_from_toml(const char *config_path, _Out_ void **error_out)",
  );
  const rrq_producer_enqueue = lib.func(
    "void * rrq_producer_enqueue(void *handle, const char *request_json, _Out_ void **error_out)",
  );
  const rrq_producer_get_job_status = lib.func(
    "void * rrq_producer_get_job_status(void *handle, const char *request_json, _Out_ void **error_out)",
  );
  const rrq_string_free = lib.func("void rrq_string_free(void *ptr)");

  return {
    rrq_producer_new,
    rrq_producer_new_from_toml,
    rrq_producer_free,
    rrq_producer_constants,
    rrq_producer_config_from_toml,
    rrq_producer_enqueue,
    rrq_producer_get_job_status,
    rrq_string_free,
    decode: koffi.decode,
  };
}

function getLibrary(): NodeLibrary {
  if (!loadedLib) {
    loadedLib = loadNodeLibrary();
  }
  return loadedLib;
}

function loadBunLibrary(): BunLibrary {
  const require = createRequire(import.meta.url);
  let dlopen: (path: string, symbols: Record<string, any>) => { symbols: any };
  let FFIType: Record<string, number>;
  let CString: new (input: string | number) => { toString(): string };
  let ptr: (value: ArrayBufferView) => number;
  try {
    ({ dlopen, FFIType, CString, ptr } = require("bun:ffi") as {
      dlopen: (path: string, symbols: Record<string, any>) => { symbols: any };
      FFIType: Record<string, number>;
      CString: new (input: string | number) => { toString(): string };
      ptr: (value: ArrayBufferView) => number;
    });
  } catch {
    throw new RustProducerError(
      "Failed to load bun:ffi; ensure Bun runtime is available for FFI calls.",
    );
  }
  const libPath = findLibraryPath();
  const lib = dlopen(libPath, {
    rrq_producer_new: { args: [FFIType.cstring, FFIType.ptr], returns: FFIType.ptr },
    rrq_producer_new_from_toml: { args: [FFIType.cstring, FFIType.ptr], returns: FFIType.ptr },
    rrq_producer_free: { args: [FFIType.ptr], returns: FFIType.void },
    rrq_producer_constants: { args: [FFIType.ptr], returns: FFIType.ptr },
    rrq_producer_config_from_toml: { args: [FFIType.cstring, FFIType.ptr], returns: FFIType.ptr },
    rrq_producer_enqueue: {
      args: [FFIType.ptr, FFIType.cstring, FFIType.ptr],
      returns: FFIType.ptr,
    },
    rrq_producer_get_job_status: {
      args: [FFIType.ptr, FFIType.cstring, FFIType.ptr],
      returns: FFIType.ptr,
    },
    rrq_string_free: { args: [FFIType.ptr], returns: FFIType.void },
  });
  return { symbols: lib.symbols, CString, ptr };
}

function getBunLibrary(): BunLibrary {
  if (!loadedBun) {
    loadedBun = loadBunLibrary();
  }
  return loadedBun;
}

function isBunRuntime(): boolean {
  return typeof (globalThis as any).Bun !== "undefined";
}

function bunAllocErrorOut(): BigUint64Array {
  const out = new BigUint64Array(1);
  out[0] = 0n;
  return out;
}

function bunTakeError(lib: BunLibrary, errOut: BigUint64Array): string | null {
  const ptrValue = errOut[0];
  if (ptrValue === 0n) {
    return null;
  }
  const message = new lib.CString(Number(ptrValue)).toString();
  lib.symbols.rrq_string_free(Number(ptrValue));
  errOut[0] = 0n;
  return message;
}

function encodeCString(value: string): Uint8Array {
  const bytes = new TextEncoder().encode(`${value}\0`);
  return bytes;
}

let cachedConstants: Record<string, unknown> | null = null;

export function getProducerConstants(): Record<string, unknown> {
  if (cachedConstants) {
    return cachedConstants;
  }

  if (isBunRuntime()) {
    const lib = getBunLibrary();
    const errOut = bunAllocErrorOut();
    const resultPtr = lib.symbols.rrq_producer_constants(lib.ptr(errOut));
    if (!resultPtr) {
      const message = bunTakeError(lib, errOut);
      throw new RustProducerError(message ?? "Failed to load producer constants");
    }
    let json: string;
    try {
      json = new lib.CString(resultPtr).toString();
    } finally {
      lib.symbols.rrq_string_free(resultPtr);
    }
    try {
      cachedConstants = JSON.parse(json) as Record<string, unknown>;
      return cachedConstants;
    } catch (error) {
      throw new RustProducerError(`Invalid response from producer: ${String(error)}`);
    }
  }

  const lib = getLibrary();
  const errOut: Array<unknown> = [null];
  const resultPtr = lib.rrq_producer_constants(errOut);
  if (!resultPtr) {
    const errPtr = errOut[0];
    const message = errPtr ? lib.decode(errPtr, "char *") : null;
    if (errPtr) {
      lib.rrq_string_free(errPtr);
    }
    throw new RustProducerError(message ?? "Failed to load producer constants");
  }
  let json: string;
  try {
    json = lib.decode(resultPtr, "char *");
  } finally {
    lib.rrq_string_free(resultPtr);
  }
  try {
    cachedConstants = JSON.parse(json) as Record<string, unknown>;
    return cachedConstants;
  } catch (error) {
    throw new RustProducerError(`Invalid response from producer: ${String(error)}`);
  }
}

export interface ProducerSettings {
  redis_dsn: string;
  queue_name: string;
  max_retries: number;
  job_timeout_seconds: number;
  result_ttl_seconds: number;
  idempotency_ttl_seconds: number;
}

export interface JobResult {
  status: string;
  result?: unknown | null;
  last_error?: string | null;
}

export interface JobStatusResponse {
  found: boolean;
  job?: JobResult | null;
}

export function loadProducerSettings(configPath?: string): ProducerSettings {
  if (isBunRuntime()) {
    const lib = getBunLibrary();
    const errOut = bunAllocErrorOut();
    const payload = configPath ? encodeCString(configPath) : new Uint8Array(0);
    const ptr = configPath ? lib.ptr(payload) : 0;
    const resultPtr = lib.symbols.rrq_producer_config_from_toml(ptr, lib.ptr(errOut));
    if (!resultPtr) {
      const message = bunTakeError(lib, errOut);
      throw new RustProducerError(message ?? "Failed to load producer settings");
    }
    let json: string;
    try {
      json = new lib.CString(resultPtr).toString();
    } finally {
      lib.symbols.rrq_string_free(resultPtr);
    }
    try {
      return JSON.parse(json) as ProducerSettings;
    } catch (error) {
      throw new RustProducerError(`Invalid response from producer: ${String(error)}`);
    }
  }

  const lib = getLibrary();
  const errOut: Array<unknown> = [null];
  const resultPtr = lib.rrq_producer_config_from_toml(configPath ?? null, errOut);
  if (!resultPtr) {
    const errPtr = errOut[0];
    const message = errPtr ? lib.decode(errPtr, "char *") : null;
    if (errPtr) {
      lib.rrq_string_free(errPtr);
    }
    throw new RustProducerError(message ?? "Failed to load producer settings");
  }
  let json: string;
  try {
    json = lib.decode(resultPtr, "char *");
  } finally {
    lib.rrq_string_free(resultPtr);
  }
  try {
    return JSON.parse(json) as ProducerSettings;
  } catch (error) {
    throw new RustProducerError(`Invalid response from producer: ${String(error)}`);
  }
}

export class RustProducer {
  private handle: unknown | number | null;
  private backend: "node" | "bun";

  private constructor(handle: unknown | number, backend: "node" | "bun") {
    this.handle = handle;
    this.backend = backend;
  }

  static fromConfig(config: Record<string, unknown>): RustProducer {
    if (isBunRuntime()) {
      const lib = getBunLibrary();
      const payload = encodeCString(JSON.stringify(config));
      const errOut = bunAllocErrorOut();
      const handle = lib.symbols.rrq_producer_new(lib.ptr(payload), lib.ptr(errOut));
      if (!handle) {
        const message = bunTakeError(lib, errOut);
        throw new RustProducerError(message ?? "Failed to create producer");
      }
      return new RustProducer(handle, "bun");
    }

    const lib = getLibrary();
    const payload = JSON.stringify(config);
    const errOut: Array<unknown> = [null];
    const handle = lib.rrq_producer_new(payload, errOut);
    if (!handle) {
      const errPtr = errOut[0];
      const message = errPtr ? lib.decode(errPtr, "char *") : null;
      if (errPtr) {
        lib.rrq_string_free(errPtr);
      }
      throw new RustProducerError(message ?? "Failed to create producer");
    }
    return new RustProducer(handle, "node");
  }

  static fromToml(configPath?: string): RustProducer {
    if (isBunRuntime()) {
      const lib = getBunLibrary();
      const errOut = bunAllocErrorOut();
      const payload = configPath ? encodeCString(configPath) : new Uint8Array(0);
      const ptr = configPath ? lib.ptr(payload) : 0;
      const handle = lib.symbols.rrq_producer_new_from_toml(ptr, lib.ptr(errOut));
      if (!handle) {
        const message = bunTakeError(lib, errOut);
        throw new RustProducerError(message ?? "Failed to create producer");
      }
      return new RustProducer(handle, "bun");
    }

    const lib = getLibrary();
    const errOut: Array<unknown> = [null];
    const handle = lib.rrq_producer_new_from_toml(configPath ?? null, errOut);
    if (!handle) {
      const errPtr = errOut[0];
      const message = errPtr ? lib.decode(errPtr, "char *") : null;
      if (errPtr) {
        lib.rrq_string_free(errPtr);
      }
      throw new RustProducerError(message ?? "Failed to create producer");
    }
    return new RustProducer(handle, "node");
  }

  close(): void {
    if (this.backend === "bun") {
      if (typeof this.handle === "number" && this.handle !== 0) {
        const lib = getBunLibrary();
        lib.symbols.rrq_producer_free(this.handle);
        this.handle = null;
      }
      return;
    }

    if (this.handle) {
      const lib = getLibrary();
      lib.rrq_producer_free(this.handle);
      this.handle = null;
    }
  }

  enqueue(request: Record<string, unknown>): Promise<Record<string, unknown>> {
    if (this.backend === "bun") {
      if (typeof this.handle !== "number" || this.handle === 0) {
        return Promise.reject(new RustProducerError("Producer handle is closed"));
      }
      const lib = getBunLibrary();
      const payload = encodeCString(JSON.stringify(request));
      const errOut = bunAllocErrorOut();
      let resultPtr: number;
      try {
        resultPtr = lib.symbols.rrq_producer_enqueue(
          this.handle,
          lib.ptr(payload),
          lib.ptr(errOut),
        );
      } catch (error) {
        return Promise.reject(error);
      }
      if (!resultPtr) {
        const message = bunTakeError(lib, errOut);
        return Promise.reject(new RustProducerError(message ?? "Enqueue failed"));
      }
      let json: string;
      try {
        json = new lib.CString(resultPtr).toString();
      } finally {
        lib.symbols.rrq_string_free(resultPtr);
      }
      try {
        return Promise.resolve(JSON.parse(json) as Record<string, unknown>);
      } catch (error) {
        return Promise.reject(
          new RustProducerError(`Invalid response from producer: ${String(error)}`),
        );
      }
    }

    if (!this.handle) {
      return Promise.reject(new RustProducerError("Producer handle is closed"));
    }
    const lib = getLibrary();
    const payload = JSON.stringify(request);

    return new Promise((resolve, reject) => {
      try {
        const errOut: Array<unknown> = [null];
        const resultPtr = lib.rrq_producer_enqueue(this.handle, payload, errOut);
        if (!resultPtr) {
          const errPtr = errOut[0];
          const message = errPtr ? lib.decode(errPtr, "char *") : null;
          if (errPtr) {
            lib.rrq_string_free(errPtr);
          }
          reject(new RustProducerError(message ?? "Enqueue failed"));
          return;
        }
        let json: string;
        try {
          json = lib.decode(resultPtr, "char *");
        } finally {
          lib.rrq_string_free(resultPtr);
        }
        resolve(JSON.parse(json) as Record<string, unknown>);
      } catch (err) {
        reject(new RustProducerError(`Invalid response from producer: ${String(err)}`));
      }
    });
  }

  getJobStatus(request: Record<string, unknown>): Promise<JobStatusResponse> {
    if (this.backend === "bun") {
      if (typeof this.handle !== "number" || this.handle === 0) {
        return Promise.reject(new RustProducerError("Producer handle is closed"));
      }
      const lib = getBunLibrary();
      const payload = encodeCString(JSON.stringify(request));
      const errOut = bunAllocErrorOut();
      let resultPtr: number;
      try {
        resultPtr = lib.symbols.rrq_producer_get_job_status(
          this.handle,
          lib.ptr(payload),
          lib.ptr(errOut),
        );
      } catch (error) {
        return Promise.reject(error);
      }
      if (!resultPtr) {
        const message = bunTakeError(lib, errOut);
        return Promise.reject(new RustProducerError(message ?? "Failed to get job status"));
      }
      let json: string;
      try {
        json = new lib.CString(resultPtr).toString();
      } finally {
        lib.symbols.rrq_string_free(resultPtr);
      }
      try {
        return Promise.resolve(JSON.parse(json) as JobStatusResponse);
      } catch (error) {
        return Promise.reject(
          new RustProducerError(`Invalid response from producer: ${String(error)}`),
        );
      }
    }

    if (!this.handle) {
      return Promise.reject(new RustProducerError("Producer handle is closed"));
    }
    const lib = getLibrary();
    const payload = JSON.stringify(request);

    return new Promise((resolve, reject) => {
      try {
        const errOut: Array<unknown> = [null];
        const resultPtr = lib.rrq_producer_get_job_status(this.handle, payload, errOut);
        if (!resultPtr) {
          const errPtr = errOut[0];
          const message = errPtr ? lib.decode(errPtr, "char *") : null;
          if (errPtr) {
            lib.rrq_string_free(errPtr);
          }
          reject(new RustProducerError(message ?? "Failed to get job status"));
          return;
        }
        let json: string;
        try {
          json = lib.decode(resultPtr, "char *");
        } finally {
          lib.rrq_string_free(resultPtr);
        }
        resolve(JSON.parse(json) as JobStatusResponse);
      } catch (err) {
        reject(new RustProducerError(`Invalid response from producer: ${String(err)}`));
      }
    });
  }
}
