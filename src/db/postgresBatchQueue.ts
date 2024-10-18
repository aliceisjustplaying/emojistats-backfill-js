import { Mutex } from 'async-mutex';

import { MAX_FLUSH_RETRIES } from '../constants.js';
import { concurrentPostgresInserts } from '../metrics.js';

export class PostgresBatchQueue<T> {
  private queue: T[] = [];
  private mutex = new Mutex();
  private batchSize: number;
  private batchTimeoutMs: number;
  private batchTimer: NodeJS.Timeout | null = null;
  private isShuttingDown = false;
  private insertFn: (batch: T[]) => Promise<void>;

  constructor(batchSize: number, batchTimeoutMs: number, insertFn: (batch: T[]) => Promise<void>) {
    this.batchSize = batchSize;
    this.batchTimeoutMs = batchTimeoutMs;
    this.insertFn = insertFn;
  }

  public async enqueue(data: T): Promise<void> {
    if (this.isShuttingDown) {
      throw new Error('Cannot enqueue data, the queue is shutting down.');
    }

    const shouldFlush = await this.mutex.runExclusive(() => {
      this.queue.push(data);

      if (this.queue.length >= this.batchSize) {
        return true;
      } else if (!this.batchTimer) {
        this.scheduleFlush();
      }

      return false;
    });

    if (shouldFlush) {
      await this.flushQueue();
    }
  }

  private scheduleFlush(): void {
    this.batchTimer = setTimeout(() => {
      this.flushQueue().catch((err: unknown) => {
        console.error(`Scheduled flush error: ${(err as Error).message}`);
      });
    }, this.batchTimeoutMs);
  }

  private async flushQueue(): Promise<void> {
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }

    let currentBatch: T[] = [];

    await this.mutex.runExclusive(() => {
      if (this.queue.length === 0) {
        return;
      }
      currentBatch = this.queue.splice(0, this.batchSize);
    });

    if (currentBatch.length === 0) {
      return;
    }

    concurrentPostgresInserts.inc();

    try {
      await this.attemptFlush(currentBatch);
      // process.stdout.write('.');
      // console.log(`Flushed batch of ${currentBatch.length} items.`);
    } catch (error) {
      console.error(`Error flushing PostgreSQL batch: ${(error as Error).message}`);
      // Re-add the failed batch back for retry
      await this.mutex.runExclusive(() => {
        this.queue = currentBatch.concat(this.queue);
      });
    } finally {
      concurrentPostgresInserts.dec();
    }
  }

  private async attemptFlush(batch: T[]): Promise<void> {
    let attempt = 0;
    let success = false;

    while (attempt < MAX_FLUSH_RETRIES && !success) {
      try {
        await this.insertFn(batch);
        success = true;
      } catch (error) {
        attempt++;
        console.error(`Flush attempt ${attempt} failed: ${(error as Error).message}`);

        if (attempt < MAX_FLUSH_RETRIES) {
          const backoffTime = 2 ** attempt * 1000;
          console.log(`Retrying in ${backoffTime} ms...`);
          await new Promise((resolve) => setTimeout(resolve, backoffTime));
        } else {
          // console.error('Max retries reached. Re-queueing the batch.');
          console.error('Max retries reached. This is bad. Exiting.');
          process.exit(1);
          // throw error; // Let the caller handle re-queueing
        }
      }
    }
  }

  public async shutdown(): Promise<void> {
    this.isShuttingDown = true;
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }
    await this.flushQueue();
    console.log('Flushed all remaining items.');
  }
}
