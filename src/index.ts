import axios from 'axios';
import 'dotenv/config';
import Database from 'libsql';
import fs from 'node:fs/promises';
import { performance } from 'node:perf_hooks';
import pLimit from 'p-limit';
import readline from 'readline';

import {
  DATA_OUTPUT_FILE,
  DIDS_TO_PROCESS,
  HEALTH_CHECK_FILE,
  PDS_DATA_FETCH_CONCURRENCY,
  PDS_HEALTH_CHECK_CONCURRENCY,
  PLC_DB_PATH,
  RELAY_URL,
  SQL_OUTPUT_FILE,
} from './constants.js';
import { isPDSHealthy, sanitizePDSName } from './helpers.js';
import logger from './logger.js';
import { BskyData, DIDsFromDB, PDSDIDGrouped, PDSHealthStatus } from './types.js';

const db = new Database(PLC_DB_PATH);

async function fetchAndDumpDidsPdses() {
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

  const plcDbFile = await fs.open(SQL_OUTPUT_FILE, 'w');
  const plcDbWriteStream = plcDbFile.createWriteStream();

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
    const processedEndpoint = endpoint
      .replace(/^(https?:\/\/)/, '')
      .replace(/\/+$/, '')
      .trim();
    const finalEndpoint =
      processedEndpoint.includes('bsky.social') || processedEndpoint.includes('bsky.network') ?
        RELAY_URL
      : processedEndpoint;

    plcDbWriteStream.write(JSON.stringify({ did, pds: finalEndpoint }) + '\n');
  }

  plcDbWriteStream.close();
  const endTime = performance.now();
  const totalTime = (endTime - startTime) / 1000;
  const averageSpeed = count / totalTime;
  logger.info(`Data dumped to ${SQL_OUTPUT_FILE}`);
  logger.info(`Total DIDs processed: ${count}`);
  logger.info(`Total time: ${totalTime.toFixed(2)} seconds`);
  logger.info(`Average speed: ${averageSpeed.toFixed(2)} DIDs/second`);
}

async function checkAllPDSHealth() {
  const startTime = performance.now();
  logger.info('Starting to process data from file');

  const readFile = await fs.open(SQL_OUTPUT_FILE, 'r');
  const fileStream = readFile.createReadStream();
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
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

    const { did, pds } = JSON.parse(line) as { did: string; pds: string };
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

    const limit = pLimit(PDS_HEALTH_CHECK_CONCURRENCY);

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

async function processDidsAndFetchData(dids: { did: string; pds: string }[]) {
  const limit = pLimit(PDS_DATA_FETCH_CONCURRENCY);
  const fetchedData: BskyData[] = [];
  let successfulRequests = 0;
  let unsuccessfulRequests = 0;
  let successfulDids = 0;
  let failedDids = 0;

  const tasks = dids.map(({ did, pds }) =>
    limit(async () => {
      try {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
        const res = await axios.post(
          'http://localhost:8000/fetch',
          { did, pds },
          {
            responseType: 'stream',
            timeout: 60000,
          },
        );

        successfulRequests++;

        await new Promise<void>((resolve, reject) => {
          let buffer = '';
          let didSucceeded = false;

          // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
          res.data.on('data', (chunk: Buffer) => {
            buffer += chunk.toString();
            let boundary = buffer.indexOf('\n');
            while (boundary !== -1) {
              const line = buffer.substring(0, boundary);
              buffer = buffer.substring(boundary + 1);
              if (line.trim()) {
                try {
                  const json = JSON.parse(line) as BskyData;
                  fetchedData.push(json);
                  didSucceeded = true;
                } catch (err) {
                  logger.error(`JSON parse error for DID ${did}: ${(err as Error).message}`);
                }
              }
              boundary = buffer.indexOf('\n');
            }
          });

          // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
          res.data.on('end', () => {
            if (buffer.trim()) {
              try {
                const json = JSON.parse(buffer) as BskyData;
                fetchedData.push(json);
                didSucceeded = true;
              } catch (err) {
                logger.error(`JSON parse error at stream end for DID ${did}: ${(err as Error).message}`);
              }
            }
            if (didSucceeded) {
              successfulDids++;
            } else {
              failedDids++;
            }
            resolve();
          });

          // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
          res.data.on('error', (err: Error) => {
            logger.error(`Stream error for DID ${did}: ${err.message}`);
            failedDids++;
            reject(err);
          });
        });
        return;
      } catch (error) {
        logger.error(`Error fetching data for DID ${did}: ${(error as Error).message}`);
        unsuccessfulRequests++;
        failedDids++;
      }
    }),
  );

  await Promise.all(tasks);
  logger.info(`Fetched data for ${fetchedData.length} DIDs.`);
  logger.info(`Successful requests: ${successfulRequests}, Unsuccessful requests: ${unsuccessfulRequests}`);
  logger.info(`Successful DIDs: ${successfulDids}, Failed DIDs: ${failedDids}`);

  const writeFile = await fs.open(DATA_OUTPUT_FILE, 'w');
  const writeStream = writeFile.createWriteStream();
  for (const data of fetchedData) {
    writeStream.write(JSON.stringify(data) + '\n');
  }
  writeStream.close();
  logger.info(`Streamed fetched data to ${DATA_OUTPUT_FILE}`);
  return fetchedData;
}

async function main() {
  if (
    !(await fs
      .access(SQL_OUTPUT_FILE)
      .then(() => true)
      .catch(() => false))
  ) {
    await fetchAndDumpDidsPdses();
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

  // Prepare the list of DIDs to process
  const allDids: { did: string; pds: string }[] = [];

  for (const [pds, dids] of Object.entries(healthyGroupedByPDS)) {
    for (const did of dids) {
      allDids.push({ did, pds });
    }
  }

  logger.info(`Total DIDs from DB: ${allDids.length}`);

  const didsToProcess = allDids.slice(0, DIDS_TO_PROCESS);
  logger.info(`Processing the first ${didsToProcess.length} DIDs for testing.`);

  const fetchedData = await processDidsAndFetchData(didsToProcess);

  logger.info(`Fetched data array length: ${fetchedData.length}`);
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
