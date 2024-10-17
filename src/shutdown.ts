// src/shutdown.ts
import { stopMetricsServer } from './metrics.js';
import { closeDatabase } from './postgres.js';
import { postgresBatchQueue } from './postgresBatchQueue.js';

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
