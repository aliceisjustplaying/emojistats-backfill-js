import { DENYLIST, DIDS_TO_PROCESS, METRICS_PORT } from './constants.js';
import { startMetricsServer } from './metrics.js';
import { redis } from './redis.js';
import { gracefulShutdown, registerShutdownHandlers } from './shutdown.js';
import { fetchAndDumpDidsPdses } from './stages/stage1.js';
import { checkAllPDSHealth, selectAllDids } from './stages/stage2.js';
import { processDidsAndFetchData } from './stages/stage3.js';
import { DidAndPds } from './types.js';

async function main() {
  // Register graceful shutdown handlers
  registerShutdownHandlers();

  // start metrics server
  startMetricsServer(METRICS_PORT);

  // connect to redis
  await redis.connect();

  // stage 1
  await fetchAndDumpDidsPdses();

  // stage 2
  const { groupedByPDS, pdsHealthStatus } = await checkAllPDSHealth();
  const allDids: DidAndPds[] = selectAllDids(groupedByPDS, pdsHealthStatus);

  // stage 3
  const didsToProcess = allDids.slice(0, DIDS_TO_PROCESS);
  const filteredDids = didsToProcess.filter((did) => !DENYLIST.includes(did.did));
  console.log(`Processing ${filteredDids.length} DIDs`);

  await processDidsAndFetchData(filteredDids);

  await gracefulShutdown();
}

main().catch(async (error: unknown) => {
  console.error(`Unexpected error: ${error instanceof Error ? error.message : String(error)}`);
  await gracefulShutdown();
  process.exit(1);
});
