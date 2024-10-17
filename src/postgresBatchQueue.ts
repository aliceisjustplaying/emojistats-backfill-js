// src/postgresBatchQueue.ts
import { Mutex } from 'async-mutex';

import { BATCH_SIZE, BATCH_TIMEOUT_MS, MAX_FLUSH_RETRIES } from './constants.js';
import { concurrentPostgresInserts } from './metrics.js';
import { closeDatabase, db } from './postgres.js';
import { PostData } from './types.js';

export class PostgresBatchQueue {
  private queue: PostData[] = [];
  private mutex = new Mutex();
  private batchSize: number;
  private batchTimeoutMs: number;
  private batchTimer: NodeJS.Timeout | null = null;
  private isShuttingDown = false;

  constructor(batchSize: number, batchTimeoutMs: number) {
    this.batchSize = batchSize;
    this.batchTimeoutMs = batchTimeoutMs;
  }

  /**
   * Adds a PostData item to the queue and triggers flush if necessary.
   * @param data PostData to enqueue
   */
  public async enqueue(data: PostData): Promise<void> {
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

  /**
   * Schedules a flush after the specified timeout.
   */
  private scheduleFlush(): void {
    this.batchTimer = setTimeout(() => {
      this.flushQueue().catch((err: unknown) => {
        console.error(`Scheduled flush error: ${(err as Error).message}`);
      });
    }, this.batchTimeoutMs);
  }

  /**
   * Flushes the current queue to PostgreSQL.
   */
  private async flushQueue(): Promise<void> {
    // Clear the existing timer
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }

    let currentBatch: PostData[] = [];

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
      process.stdout.write('.');
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

  /**
   * Attempts to flush the batch with retry logic.
   * @param batch The batch of PostData to flush
   */
  private async attemptFlush(batch: PostData[]): Promise<void> {
    let attempt = 0;
    let success = false;

    while (attempt < MAX_FLUSH_RETRIES && !success) {
      try {
        await db.transaction().execute(async (tx) => {
          await tx
            .insertInto('posts') // Ensure 'posts' is your table name
            .values(
              batch.map((post) => ({
                cid: post.cid,
                did: post.did,
                rkey: post.rkey,
                has_emojis: post.hasEmojis,
                langs: post.langs,
                text: post.post,
                created_at: post.createdAt,
              })),
            )
            .execute();
        });

        success = true;
      } catch (error) {
        attempt++;
        console.error(`Flush attempt ${attempt} failed: ${(error as Error).message}`);

        if (attempt < MAX_FLUSH_RETRIES) {
          const backoffTime = 2 ** attempt * 1000; // Exponential backoff
          console.log(`Retrying in ${backoffTime} ms...`);
          await new Promise((resolve) => setTimeout(resolve, backoffTime));
        } else {
          console.error('Max retries reached. Re-queueing the batch.');
          throw error; // Let the caller handle re-queueing
        }
      }
    }
  }

  /**
   * Gracefully shuts down the queue by flushing remaining items.
   */
  public async shutdown(): Promise<void> {
    this.isShuttingDown = true;
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }
    await this.flushQueue();
    console.log('Flushed all remaining items.');
    await closeDatabase();
    console.log('Database connections closed.');
  }
}

// Instantiate the queue
export const postgresBatchQueue = new PostgresBatchQueue(BATCH_SIZE, BATCH_TIMEOUT_MS);
