// import { monitorPgPool } from '@christiangalsterer/node-postgres-prometheus-exporter';
import express from 'express';
import { Registry, collectDefaultMetrics } from 'prom-client';

import logger from './logger.js';
// import { pool } from './postgres.js';

const register = new Registry();
collectDefaultMetrics({ register });

// monitorPgPool(pool, register);

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

export const startMetricsServer = (port: number, host = '127.0.0.1') => {
  const server = app.listen(port, host, () => {
    console.log(`Metrics server listening on port ${port}`);
  });

  server.on('close', () => {
    console.log('Metrics server closed.');
  });

  return server;
};
