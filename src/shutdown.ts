// src/shutdown.ts
import { closeDatabase } from './db/postgres.js';
import { postBatchQueue, profileBatchQueue } from './db/postgresBatchQueues.js';
import { stopMetricsServer } from './metrics.js';
import { redis } from './redis.js';

export async function gracefulShutdown(): Promise<void> {
  console.log('Initiating graceful shutdown...');
  try {
    await stopMetricsServer();
    await Promise.all([postBatchQueue.shutdown(), profileBatchQueue.shutdown()]);
    console.log('All pending batches have been flushed.');
    await closeDatabase();
    console.log('Database connections closed.');
    await redis.quit();
    process.exit(0);
  } catch (err) {
    console.error(`Error during shutdown: ${(err as Error).message}`);
    process.exit(1);
  }
}

// Register shutdown handlers
export function registerShutdownHandlers() {
  process.on('SIGINT', () => void gracefulShutdown());
  process.on('SIGTERM', () => void gracefulShutdown());
  process.on('uncaughtException', (err) => {
    console.error('Uncaught Exception:', err);
    void gracefulShutdown();
  });
  process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
    void gracefulShutdown();
  });
}
