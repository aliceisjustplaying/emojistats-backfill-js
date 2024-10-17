// src/shutdown.ts

import { postgresBatchQueue } from './postgresBatchQueue.js';
import { closeDatabase } from './postgres.js';
import { stopMetricsServer } from './metrics.js';

export async function gracefulShutdown(): Promise<void> {
  console.log('Initiating graceful shutdown...');
  try {
    await stopMetricsServer();
    await postgresBatchQueue.shutdown();
    console.log('All pending batches have been flushed.');
    await closeDatabase();
    console.log('Database connections closed.');
    process.exit(0);
  } catch (err) {
    console.error(`Error during shutdown: ${(err as Error).message}`);
    process.exit(1);
  }
}

// Register shutdown handlers
export function registerShutdownHandlers() {
  process.on('SIGINT', gracefulShutdown);
  process.on('SIGTERM', gracefulShutdown);
  process.on('uncaughtException', async (err) => {
    console.error('Uncaught Exception:', err);
    await gracefulShutdown();
  });
  process.on('unhandledRejection', async (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
    await gracefulShutdown();
  });
}
