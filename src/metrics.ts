import { monitorPgPool } from '@christiangalsterer/node-postgres-prometheus-exporter';
import express from 'express';
import { Server } from 'http';
import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from 'prom-client';

import { pool } from './db/postgres.js';

const register = new Registry();
collectDefaultMetrics({ register });

export const concurrentPostgresInserts = new Gauge({
  name: 'bluesky_concurrent_postgres_inserts',
  help: 'Number of concurrent Postgres inserts',
  registers: [register],
});

export const didsProcessedTotal = new Counter({
  name: 'bluesky_dids_processed_total',
  help: 'Total number of DIDs processed',
  registers: [register],
});

export const didsSuccessfulTotal = new Counter({
  name: 'bluesky_dids_successful_total',
  help: 'Total number of successfully processed DIDs',
  registers: [register],
});

export const didsFailedTotal = new Counter({
  name: 'bluesky_dids_failed_total',
  help: 'Total number of failed DIDs',
  registers: [register],
});

export const didsRetryTotal = new Counter({
  name: 'bluesky_dids_retry_total',
  help: 'Total number of DIDs retried',
  registers: [register],
});

export const didsProcessingDuration = new Histogram({
  name: 'bluesky_dids_processing_duration_seconds',
  help: 'Duration of DID processing in seconds',
  buckets: [0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1200, 1800],
  registers: [register],
});

export const didsConcurrentProcessing = new Gauge({
  name: 'bluesky_dids_concurrent_processing',
  help: 'Number of DIDs currently being processed',
  registers: [register],
});

export const concurrentRedisInserts = new Gauge({
  name: 'bluesky_concurrent_redis_inserts',
  help: 'Number of concurrent Redis inserts',
  registers: [register],
});

export const totalPostsWithEmojis = new Counter({
  name: 'bluesky_total_posts_with_emojis',
  help: 'Total number of posts with emojis',
  registers: [register],
});

export const totalPostsWithoutEmojis = new Counter({
  name: 'bluesky_total_posts_without_emojis',
  help: 'Total number of posts without emojis',
  registers: [register],
});

export const totalEmojis = new Counter({
  name: 'bluesky_total_emojis',
  help: 'Total number of emojis processed',
  registers: [register],
});

export const totalProcessedPosts = new Counter({
  name: 'bluesky_total_posts_processed_for_emojis',
  help: 'Total number of posts processed for emojis',
  registers: [register],
});

export const totalDisplayNamesProcessed = new Counter({
  name: 'bluesky_total_display_names_processed_for_emojis',
  help: 'Total number of display names processed for emojis',
  registers: [register],
});

export const totalDescriptionNamesProcessed = new Counter({
  name: 'bluesky_total_description_names_processed_for_emojis',
  help: 'Total number of description names processed for emojis',
  registers: [register],
});

export const totalDisplayNamesWithEmojis = new Counter({
  name: 'bluesky_total_display_names_with_emojis',
  help: 'Total number of display names with emojis',
  registers: [register],
});

export const totalDescriptionNamesWithEmojis = new Counter({
  name: 'bluesky_total_description_names_with_emojis',
  help: 'Total number of description names with emojis',
  registers: [register],
});

export const totalProcessedProfiles = new Counter({
  name: 'bluesky_total_processed_profiles',
  help: 'Total number of processed profiles',
  registers: [register],
});

export const totalDisplayNamesWithoutEmojis = new Counter({
  name: 'bluesky_total_display_names_without_emojis',
  help: 'Total number of display names without emojis',
  registers: [register],
});

export const totalDescriptionNamesWithoutEmojis = new Counter({
  name: 'bluesky_total_description_names_without_emojis',
  help: 'Total number of description names without emojis',
  registers: [register],
});

export const totalDisplayNameEmojis = new Counter({
  name: 'bluesky_total_display_name_emojis',
  help: 'Total number of display name emojis',
  registers: [register],
});

export const totalDescriptionNameEmojis = new Counter({
  name: 'bluesky_total_description_name_emojis',
  help: 'Total number of description name emojis',
  registers: [register],
});

monitorPgPool(pool, register);

const app = express();

app.get('/metrics', (req, res) => {
  register
    .metrics()
    .then((metrics) => {
      res.set('Content-Type', register.contentType);
      res.send(metrics);
    })
    .catch((ex: unknown) => {
      console.error(`Error serving metrics: ${(ex as Error).message}`);
      res.status(500).end((ex as Error).message);
    });
});

let metricsServer: Server;

export const startMetricsServer = (port: number, host = '127.0.0.1') => {
  metricsServer = app.listen(port, host, () => {
    console.log(`Metrics server listening on port ${port}`);
  });

  metricsServer.on('close', () => {
    console.log('Metrics server closed.');
  });

  return metricsServer;
};

export function stopMetricsServer(): Promise<void> {
  return new Promise((resolve, reject) => {
    metricsServer.close((err) => {
      if (err) {
        console.error('Error shutting down metrics server:', err);
        reject(err);
      } else {
        console.log('Metrics server shut down successfully.');
        resolve();
      }
    });
  });
}
