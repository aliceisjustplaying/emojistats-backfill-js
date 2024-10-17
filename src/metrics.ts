import { monitorPgPool } from '@christiangalsterer/node-postgres-prometheus-exporter';
import express from 'express';
import { Server } from 'http';
import { Gauge, Registry, collectDefaultMetrics } from 'prom-client';

import { pool } from './db/postgres.js';

const register = new Registry();
collectDefaultMetrics({ register });

export const concurrentPostgresInserts = new Gauge({
  name: 'bluesky_concurrent_postgres_inserts',
  help: 'Number of concurrent Postgres inserts',
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
