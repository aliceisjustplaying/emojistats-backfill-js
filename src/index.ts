import 'dotenv/config';
import Database from 'libsql';
import { open } from 'node:fs/promises';
import fs from 'node:fs/promises';
import readline from 'readline';
import pLimit from 'p-limit';

import { PLCDbPath, relay } from './constants.js';
import { DIDsFromDB, PDSDIDGrouped, PDSHealthStatus } from './types.js';
import logger from './logger.js';
import { isPDSHealthy } from './helpers.js';

const db = new Database(PLCDbPath);
const OUTPUT_FILE = 'dids_pds.ndjson';
const HEALTH_CHECK_FILE = 'pds_health.json';

async function fetchAndDumpData() {
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

  const writeFile = await open(OUTPUT_FILE, 'w');
  const writeStream = writeFile.createWriteStream();

  for (const row of didquery.iterate()) {
    const { did, endpoint } = row as DIDsFromDB;
    const processedEndpoint = endpoint.replace(/^(https?:\/\/)/, '');
    const finalEndpoint = processedEndpoint.includes('bsky.social') || processedEndpoint.includes('bsky.network')
      ? relay
      : processedEndpoint;
    
    writeStream.write(JSON.stringify({ did, pds: finalEndpoint }) + '\n');
  }

  writeStream.close();
  logger.info(`Data dumped to ${OUTPUT_FILE}`);
}

async function processDataFromFile() {
  const readFile = await open(OUTPUT_FILE, 'r');
  const fileStream = readFile.createReadStream();
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });

  const groupedByPDS: PDSDIDGrouped = {};

  for await (const line of rl) {
    const { did, pds } = JSON.parse(line);
    if (!groupedByPDS[pds]) {
      groupedByPDS[pds] = [];
    }
    groupedByPDS[pds].push(did);
  }

  let pdsHealthStatus: PDSHealthStatus = {};

  try {
    const healthData = await fs.readFile(HEALTH_CHECK_FILE, 'utf-8');
    pdsHealthStatus = JSON.parse(healthData) as PDSHealthStatus;
    logger.info('Loaded health check data from file');
  } catch {
    logger.info('No existing health check data found, performing health checks');

    const limit = pLimit(50);
    
    logger.info('Checking PDS health status');

    const healthCheckPromises = Object.keys(groupedByPDS).map((pds) =>
      limit(async () => {
        const healthy = await isPDSHealthy(pds);
        pdsHealthStatus[pds] = healthy;
        logger.info(`PDS ${pds} is healthy: ${healthy}`);
      }),
    );

    await Promise.all(healthCheckPromises);

    await fs.writeFile(HEALTH_CHECK_FILE, JSON.stringify(pdsHealthStatus, null, 2));
    logger.info('Health check data saved to file');
  }

  const PDSCount = Object.keys(groupedByPDS).length;
  const healthyCount = Object.values(pdsHealthStatus).filter(Boolean).length;
  const unhealthyCount = Object.values(pdsHealthStatus).length - healthyCount;
  logger.info(`Total PDS count: ${PDSCount}`);
  logger.info(`Healthy PDS count: ${healthyCount}`);
  logger.info(`Unhealthy PDS count: ${unhealthyCount}`);

  return { groupedByPDS, pdsHealthStatus };
}

async function main() {
  if (!await fs.access(OUTPUT_FILE).then(() => true).catch(() => false)) {
    await fetchAndDumpData();
  }
  const { groupedByPDS, pdsHealthStatus } = await processDataFromFile();

  const healthyGroupedByPDS = Object.entries(groupedByPDS).reduce<PDSDIDGrouped>((acc, [pds, dids]) => {
    if (pdsHealthStatus[pds]) {
      acc[pds] = dids;
    }
    return acc;
  }, {});

  const unhealthyGroupedByPDS = Object.entries(groupedByPDS).reduce<PDSDIDGrouped>((acc, [pds, dids]) => {
    if (!pdsHealthStatus[pds]) {
      acc[pds] = dids;
    }
    return acc;
  }, {});

  const totalHealthyDIDs = Object.values(healthyGroupedByPDS).flat().length;
  const totalUnhealthyDIDs = Object.values(unhealthyGroupedByPDS).flat().length;

  logger.info(`Total DIDs from healthy PDSes: ${totalHealthyDIDs}`);
  logger.info(`Total DIDs from unhealthy PDSes: ${totalUnhealthyDIDs}`);
}

main().catch((error: unknown) => {
  logger.error(`Unexpected error: ${error instanceof Error ? error.message : String(error)}`);
  process.exit(1);
});

// Keep the existing shutdown logic

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
