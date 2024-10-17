import { HEALTH_CHECK_FILE, PDS_HEALTH_CHECK_CONCURRENCY, SQL_OUTPUT_FILE } from "../constants.js";
import fs from "node:fs/promises";
import readline from "node:readline";
import pLimit from "p-limit";
import { isPDSHealthy, sanitizePDSName } from "../helpers.js";
import { PdsToDidsMap, PdsHealthStatus } from "../types.js";


export async function checkAllPDSHealth() {
  const startTime = performance.now();
  console.log('Loading DIDs from file');

  const readFile = await fs.open(SQL_OUTPUT_FILE, 'r');
  const fileStream = readFile.createReadStream();
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  const groupedByPDS: PdsToDidsMap = {};

  let lineCount = 0;
  let lastLogTime = performance.now();
  for await (const line of rl) {
    lineCount++;
    if (lineCount % 1000000 === 0) {
      const currentTime = performance.now();
      const elapsedTime = currentTime - lastLogTime;
      const linesPerSecond = 1000000 / (elapsedTime / 1000);
      console.log(`Processed ${lineCount} lines (${linesPerSecond.toFixed(2)} lines/sec)`);
      lastLogTime = currentTime;
    }

    const { did, pds } = JSON.parse(line) as { did: string; pds: string };
     
    if (!groupedByPDS[pds]) {
      groupedByPDS[pds] = [];
    }
    groupedByPDS[pds].push(did);
  }

  console.log(`Finished processing file. Total lines processed: ${lineCount}`);

  let pdsHealthStatus: PdsHealthStatus = {};

  const healthCheckStartTime = performance.now();
  try {
    const healthData = await fs.readFile(HEALTH_CHECK_FILE, 'utf-8');
    pdsHealthStatus = JSON.parse(healthData) as PdsHealthStatus;
    console.log('Loaded health check data from file');
  } catch {
    console.log('No existing health check data found, performing health checks');

    const limit = pLimit(PDS_HEALTH_CHECK_CONCURRENCY);

    console.log('Checking PDS health status');

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

    console.log(`Sanitization removed ${failedCount} invalid PDSes out of ${originalPDSCount}`);

    const healthCheckPromises = Array.from(sanitizedPDSMap.entries()).map(([sanitizedPDS]) =>
      limit(async () => {
        const healthy = await isPDSHealthy(sanitizedPDS);
        pdsHealthStatus[sanitizedPDS] = healthy;
        console.log(`PDS ${sanitizedPDS} is healthy: ${healthy}`);
      }),
    );

    await Promise.all(healthCheckPromises);

    await fs.writeFile(HEALTH_CHECK_FILE, JSON.stringify(pdsHealthStatus, null, 2));
    console.log('Health check data saved to file');

    const healthCheckEndTime = performance.now();
    const healthCheckTime = (healthCheckEndTime - healthCheckStartTime) / 1000;
    console.log(`Health check process took ${healthCheckTime.toFixed(2)} seconds`);
  
    const healthyCount = Object.values(pdsHealthStatus).filter(Boolean).length;
    const unhealthyCount = Object.values(pdsHealthStatus).length - healthyCount;
    console.log(`Total PDS count: ${sanitizedPDSMap.size}`);
    console.log(`Healthy PDS count: ${healthyCount}`);
    console.log(`Unhealthy PDS count: ${unhealthyCount}`);
  
    const endTime = performance.now();
    const totalTime = (endTime - startTime) / 1000;
    console.log(`Total processing time: ${totalTime.toFixed(2)} seconds`);  
  }

  return { groupedByPDS, pdsHealthStatus };
}

export function selectAllDids(groupedByPDS: PdsToDidsMap, pdsHealthStatus: PdsHealthStatus) {
  const healthyGroupedByPDS = Object.entries(groupedByPDS).reduce<PdsToDidsMap>((acc, [pds, dids]) => {
    if (pdsHealthStatus[pds]) {
      acc[pds] = dids;
    }
    return acc;
  }, {});

  const unhealthyGroupedByPDS = Object.entries(groupedByPDS).reduce<PdsToDidsMap>((acc, [pds, dids]) => {
    if (!pdsHealthStatus[pds]) {
      acc[pds] = dids;
    }
    return acc;
  }, {});

  const totalHealthyDIDs = Object.values(healthyGroupedByPDS).flat().length;
  const totalUnhealthyDIDs = Object.values(unhealthyGroupedByPDS).flat().length;

  console.log(`Total DIDs from healthy PDSes: ${totalHealthyDIDs}`);
  console.log(`Total DIDs from unhealthy PDSes: ${totalUnhealthyDIDs}`);

  // Prepare the list of DIDs to process
  const allDids: { did: string; pds: string; }[] = [];

  for (const [pds, dids] of Object.entries(healthyGroupedByPDS)) {
    for (const did of dids!) {
      allDids.push({ did, pds });
    }
  }

  console.log(`Total DIDs from DB: ${allDids.length}`);
  return allDids;
}
