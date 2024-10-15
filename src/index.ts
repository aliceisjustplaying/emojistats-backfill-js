import 'dotenv/config';
import Database from 'libsql';
import { open } from 'node:fs/promises';
import fs from 'node:fs/promises';
import readline from 'readline';
import pLimit from 'p-limit';

import { PLCDbPath, relay } from './constants.js';
import { DIDsFromDB, PDSDIDGrouped, PDSHealthStatus } from './types.js';
import logger from './logger.js';
import { isPDSHealthy, sanitizePDSName } from './helpers.js';

const db = new Database(PLCDbPath);
const OUTPUT_FILE = 'dids_pds.jsonl';
const HEALTH_CHECK_FILE = 'pds_health.json';

import { performance } from 'node:perf_hooks';

async function fetchAndDumpData() {
  logger.info('Fetching DIDs from database');
  const startTime = performance.now();

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

  let count = 0;
  let lastLogTime = performance.now();
  for (const row of didquery.iterate()) {
    count += 1;
    if (count % 1000000 === 0) {
      const currentTime = performance.now();
      const elapsedTime = currentTime - lastLogTime;
      const recordsPerSecond = 1000000 / (elapsedTime / 1000);
      logger.info(`Processed ${count} DIDs (${recordsPerSecond.toFixed(2)} records/sec)`);
      lastLogTime = currentTime;
    }
    const { did, endpoint } = row as DIDsFromDB;
    const processedEndpoint = endpoint.replace(/^(https?:\/\/)/, '').replace(/\/+$/, '').trim();
    const finalEndpoint = processedEndpoint.includes('bsky.social') || processedEndpoint.includes('bsky.network')
      ? relay
      : processedEndpoint;
    
    writeStream.write(JSON.stringify({ did, pds: finalEndpoint }) + '\n');
  }

  writeStream.close();
  const endTime = performance.now();
  const totalTime = (endTime - startTime) / 1000;
  const averageSpeed = count / totalTime;
  logger.info(`Data dumped to ${OUTPUT_FILE}`);
  logger.info(`Total DIDs processed: ${count}`);
  logger.info(`Total time: ${totalTime.toFixed(2)} seconds`);
  logger.info(`Average speed: ${averageSpeed.toFixed(2)} DIDs/second`);
}

async function checkAllPDSHealth() {
  const startTime = performance.now();
  logger.info('Starting to process data from file');

  const readFile = await open(OUTPUT_FILE, 'r');
  const fileStream = readFile.createReadStream();
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });

  const groupedByPDS: PDSDIDGrouped = {};

  let lineCount = 0;
  let lastLogTime = performance.now();
  for await (const line of rl) {
    lineCount++;
    if (lineCount % 1000000 === 0) {
      const currentTime = performance.now();
      const elapsedTime = currentTime - lastLogTime;
      const linesPerSecond = 1000000 / (elapsedTime / 1000);
      logger.info(`Processed ${lineCount} lines (${linesPerSecond.toFixed(2)} lines/sec)`);
      lastLogTime = currentTime;
    }

    const { did, pds } = JSON.parse(line) as { did: string, pds: string };
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if (!groupedByPDS[pds]) {
      groupedByPDS[pds] = [];
    }
    groupedByPDS[pds].push(did);
  }

  logger.info(`Finished processing file. Total lines processed: ${lineCount}`);

  let pdsHealthStatus: PDSHealthStatus = {};

  const healthCheckStartTime = performance.now();
  try {
    const healthData = await fs.readFile(HEALTH_CHECK_FILE, 'utf-8');
    pdsHealthStatus = JSON.parse(healthData) as PDSHealthStatus;
    logger.info('Loaded health check data from file');
  } catch {
    logger.info('No existing health check data found, performing health checks');

    const limit = pLimit(20);
    
    logger.info('Checking PDS health status');

    const sanitizedPDSMap = new Set<string>();
    let failedCount = 0;
    const originalPDSCount = Object.keys(groupedByPDS).length;

    for (const pds of Object.keys(groupedByPDS)) {
      try {
        const sanitizedPDS = sanitizePDSName(pds);
        sanitizedPDSMap.add(sanitizedPDS);
      } catch {
        failedCount++;
      }
    }

    logger.info(`Sanitization removed ${failedCount} invalid PDSes out of ${originalPDSCount}`);

    const healthCheckPromises = Array.from(sanitizedPDSMap.entries()).map(([sanitizedPDS]) =>
      limit(async () => {
        const healthy = await isPDSHealthy(sanitizedPDS);
        pdsHealthStatus[sanitizedPDS] = healthy;
        logger.info(`PDS ${sanitizedPDS} is healthy: ${healthy}`);
      }),
    );

    await Promise.all(healthCheckPromises);

    await fs.writeFile(HEALTH_CHECK_FILE, JSON.stringify(pdsHealthStatus, null, 2));
    logger.info('Health check data saved to file');
  }
  const healthCheckEndTime = performance.now();
  const healthCheckTime = (healthCheckEndTime - healthCheckStartTime) / 1000;
  logger.info(`Health check process took ${healthCheckTime.toFixed(2)} seconds`);

  const PDSCount = Object.keys(groupedByPDS).length;
  const healthyCount = Object.values(pdsHealthStatus).filter(Boolean).length;
  const unhealthyCount = Object.values(pdsHealthStatus).length - healthyCount;
  logger.info(`Total PDS count: ${PDSCount}`);
  logger.info(`Healthy PDS count: ${healthyCount}`);
  logger.info(`Unhealthy PDS count: ${unhealthyCount}`);

  const endTime = performance.now();
  const totalTime = (endTime - startTime) / 1000;
  logger.info(`Total processing time: ${totalTime.toFixed(2)} seconds`);

  return { groupedByPDS, pdsHealthStatus };
}

async function main() {
  if (!await fs.access(OUTPUT_FILE).then(() => true).catch(() => false)) {
    await fetchAndDumpData();
  }
  const { groupedByPDS, pdsHealthStatus } = await checkAllPDSHealth();

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
