export interface RRQSettings {
  redisDsn: string;
  defaultQueueName?: string;
  defaultMaxRetries?: number;
  defaultJobTimeoutSeconds?: number;
  defaultResultTtlSeconds?: number;
  defaultUniqueJobLockTtlSeconds?: number;
}

export const DEFAULT_SETTINGS: RRQSettings = {
  redisDsn: "redis://localhost:6379/0",
};

export function resolveSettings(overrides?: Partial<RRQSettings>): RRQSettings {
  return {
    ...DEFAULT_SETTINGS,
    ...overrides,
  };
}
