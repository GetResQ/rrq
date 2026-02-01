import { RRQClient } from "../src/producer.js";

function parseCount(value: string | undefined, fallback: number): number {
  if (!value) {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isNaN(parsed) ? fallback : parsed;
}

const redisDsn = process.env.RRQ_REDIS_DSN ?? "redis://localhost:6379/3";
const queueName = process.env.RRQ_QUEUE ?? "default";
const functionName = process.env.RRQ_FUNCTION ?? "quick_task";
const count = parseCount(process.env.RRQ_COUNT, 5);

const client = new RRQClient({
  config: {
    redisDsn,
    queueName,
  },
});

try {
  for (let i = 0; i < count; i += 1) {
    await client.enqueue(functionName, {
      params: { message: `from-ts-${i}`, source: "ts" },
    });
  }
} finally {
  await client.close();
}

console.log(`Enqueued ${count} jobs into ${queueName}`);
