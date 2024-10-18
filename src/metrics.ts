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
