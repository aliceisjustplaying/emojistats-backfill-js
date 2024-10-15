import 'dotenv/config';
import Database from 'libsql';
import pLimit from 'p-limit';

import { PLCDbPath, relay } from './constants.js';
import { DIDsFromDB, PDSDIDGrouped, PDSHealthStatus } from './types.js';

import logger from './logger.js';
import { isPDSHealthy } from './helpers.js';

const db = new Database(PLCDbPath);

let dids: DIDsFromDB[];
let groupedByPDS: PDSDIDGrouped;

logger.info('Fetching DIDs from database');
const didquery = db.prepare(`
SELECT
   identity.did,
   atproto_pds.endpoint
FROM
   identity
JOIN
   plc_log ON identity.identity_id = plc_log.identity
JOIN
   atproto_pds ON plc_log.atproto_pds = atproto_pds.pds_id
WHERE
   plc_log.entry_id IN (
      SELECT MAX(entry_id)
      FROM plc_log
      GROUP BY identity
   )
ORDER BY identity.did ASC
`);

dids = didquery.all() as DIDsFromDB[];

logger.info(`Fetched ${dids.length} DIDs from database`);

dids = dids.map(({ did, endpoint }) => ({
  did,
  endpoint: endpoint.replace(/^(https?:\/\/)/, ''),
}));

groupedByPDS = dids.reduce<PDSDIDGrouped>((acc, { did, endpoint }) => {
  if (endpoint.includes('bsky.social') || endpoint.includes('bsky.network')) {
    endpoint = relay;
  }

  acc[endpoint] = acc[endpoint] ?? [];
  acc[endpoint].push(did);
  return acc;
}, {});

const PDSCount = Object.keys(groupedByPDS).length;
logger.info(`Total PDS count: ${PDSCount}`);


const limit = pLimit(50);
const pdsHealthStatus: PDSHealthStatus = {};

logger.info('Checking PDS health status');

const healthCheckPromises = Object.keys(groupedByPDS).map((pds) =>
  limit(async () => {
    const healthy = await isPDSHealthy(pds);
    pdsHealthStatus[pds] = healthy;
    logger.info(`PDS ${pds} is healthy: ${healthy}`);
  }),
);

await Promise.all(healthCheckPromises);

logger.info('All PDS health checks completed.');

const healthyCount = Object.values(pdsHealthStatus).filter(Boolean).length;
const unhealthyCount = Object.values(pdsHealthStatus).length - healthyCount;
logger.info(`Healthy PDS count: ${healthyCount}`);
logger.info(`Unhealthy PDS count: ${unhealthyCount}`);

let isShuttingDown = false;

async function shutdown() {
  if (isShuttingDown) {
    logger.info('Shutdown called but one is already in progress.');
    return;
  }

  isShuttingDown = true;

  logger.info('Shutting down gracefully...');

  process.exit(0);
}

process.on('SIGINT', () => {
  shutdown().catch((error: unknown) => {
    logger.error(`Shutdown failed: ${(error as Error).message}`);
    process.exit(1);
  });
});

process.on('SIGTERM', () => {
  shutdown().catch((error: unknown) => {
    logger.error(`Shutdown failed: ${(error as Error).message}`);
    process.exit(1);
  });
});
