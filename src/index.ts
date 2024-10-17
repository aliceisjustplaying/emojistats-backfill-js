import 'dotenv/config';

import {
  DIDS_TO_PROCESS,
} from './constants.js';
import logger from './logger.js';
import { fetchAndDumpDidsPdses } from './stages/stage1.js';
import { processDidsAndFetchData } from './stages/stage3.js';
import { checkAllPDSHealth, selectAllDids } from './stages/stage2.js';



async function main() {
  // stage 1
  await fetchAndDumpDidsPdses();

  // stage 2
  const { groupedByPDS, pdsHealthStatus } = await checkAllPDSHealth();
  const allDids: { did: string; pds: string; }[] = selectAllDids(groupedByPDS, pdsHealthStatus);

  // stage 3
  const didsToProcess = allDids.slice(0, DIDS_TO_PROCESS);
  console.log(`Processing ${didsToProcess.length} DIDs`);

  const fetchedData = await processDidsAndFetchData(didsToProcess);

  console.log(`Fetched data array length: ${fetchedData.length}`);
}

main().catch((error: unknown) => {
  console.error(`Unexpected error: ${error instanceof Error ? error.message : String(error)}`);
  process.exit(1);
});

let isShuttingDown = false;


async function shutdown() {
  if (isShuttingDown) {
    console.log('Shutdown called but one is already in progress.');
    return;
  }

  isShuttingDown = true;

  console.log('Shutting down gracefully...');

  process.exit(0);
}

process.on('SIGINT', () => {
  shutdown().catch((error: unknown) => {
    console.error(`Shutdown failed: ${(error as Error).message}`);
    process.exit(1);
  });
});

process.on('SIGTERM', () => {
  shutdown().catch((error: unknown) => {
    console.error(`Shutdown failed: ${(error as Error).message}`);
    process.exit(1);
  });
});
