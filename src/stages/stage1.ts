import Database from 'libsql';
import fs from 'node:fs/promises';
import { performance } from 'node:perf_hooks';

import { PLC_DB_PATH, RELAY_URL, SQL_OUTPUT_FILE } from '../constants.js';
import { DidAndPds } from '../types.js';

const didDb = new Database(PLC_DB_PATH);

export async function fetchAndDumpDidsPdses() {
  try {
    await fs.access(SQL_OUTPUT_FILE);
    console.log(`${SQL_OUTPUT_FILE} already exists. Skipping DID fetching.`);
    return;
  } catch {
    console.log('fetchAndDumpDidsPdses');
    console.log('Fetching DIDs from database');
    const startTime = performance.now();

    const didquery = didDb.prepare(`
    SELECT
      identity.did as did,
      atproto_pds.endpoint as pds
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
        console.log(`Processed ${count} DIDs (${recordsPerSecond.toFixed(2)} records/sec)`);
        lastLogTime = currentTime;
      }
      const { did, pds } = row as DidAndPds;
      const sanitizedPds = pds
        .replace(/^(https?:\/\/)/, '')
        .replace(/\/+$/, '')
        .trim();
      const finalPds =
        sanitizedPds.includes('bsky.social') || sanitizedPds.includes('bsky.network') ? RELAY_URL : sanitizedPds;

      plcDbWriteStream.write(JSON.stringify({ did, pds: finalPds }) + '\n');
    }

    plcDbWriteStream.close();
    const endTime = performance.now();
    const totalTime = (endTime - startTime) / 1000;
    const averageSpeed = count / totalTime;
    console.log(`Data dumped to ${SQL_OUTPUT_FILE}`);
    console.log(`Total DIDs processed: ${count}`);
    console.log(`Total time: ${totalTime.toFixed(2)} seconds`);
    console.log(`Average speed: ${averageSpeed.toFixed(2)} DIDs/second`);
  }
}
