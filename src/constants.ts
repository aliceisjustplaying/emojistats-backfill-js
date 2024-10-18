import 'dotenv/config';

export const RELAY_URL = process.env.RELAY_URL!;
export const PLC_DB_PATH = process.env.PLC_DB_PATH!;
export const METRICS_PORT = parseInt(process.env.METRICS_PORT!, 10);
export const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';
export const SQL_OUTPUT_FILE = 'dids_pds.jsonl';
export const HEALTH_CHECK_FILE = 'pds_health.json';
export const DATA_OUTPUT_FILE = 'bsky_data.jsonl';
export const SUCCESSFUL_DIDS_LOG_INTERVAL = 1000;
export const PDS_HEALTH_CHECK_CONCURRENCY = 50;
export const PDS_HEALTH_CHECK_TIMEOUT_MS = 20000;
export const PDS_DATA_FETCH_CONCURRENCY = 200;
export const DIDS_TO_PROCESS = parseInt(process.argv[2], 10) || 10000;
export const PYTHON_SERVICE_TIMEOUT_MS = 30 * 60 * 1000; // 30 minutes
export const BATCH_SIZE = 1000; // Number of postgres records per batch
export const EMOJI_BATCH_SIZE = 1000; // Number of emoji records per batch
export const BATCH_TIMEOUT_MS = 1000; // how long to wait for a batch to fill before flushing
export const MAX_FLUSH_RETRIES = 5; // Maximum number of retry attempts for flushing
