import 'dotenv/config';

export const RELAY_URL = process.env.RELAY_URL!;
export const PLC_DB_PATH = process.env.PLC_DB_PATH!;
export const METRICS_PORT = parseInt(process.env.METRICS_PORT!, 10);
export const SQL_OUTPUT_FILE = 'dids_pds.jsonl';
export const HEALTH_CHECK_FILE = 'pds_health.json';
export const DATA_OUTPUT_FILE = 'bsky_data.jsonl';
export const PDS_HEALTH_CHECK_CONCURRENCY = 50;
export const PDS_HEALTH_CHECK_TIMEOUT_MS = 20000;
export const PDS_DATA_FETCH_CONCURRENCY = 120;
export const DIDS_TO_PROCESS = parseInt(process.argv[2], 10) || 10000;
export const PYTHON_SERVICE_TIMEOUT_MS = 30 * 60 * 1000; // 30 minutes
export const BATCH_SIZE = 5000; // Number of postgres records per batch
export const BATCH_TIMEOUT_MS = 5000; // how long to wait for a batch to fill before flushing
export const MAX_FLUSH_RETRIES = 5; // Maximum number of retry attempts for flushing
