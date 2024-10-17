import 'dotenv/config';

export const RELAY_URL = process.env.RELAY_URL!;
export const PLC_DB_PATH = process.env.PLC_DB_PATH!;
export const METRICS_PORT = parseInt(process.env.METRICS_PORT!, 10);
export const SQL_OUTPUT_FILE = 'dids_pds.jsonl';
export const HEALTH_CHECK_FILE = 'pds_health.json';
export const DATA_OUTPUT_FILE = 'bsky_data.jsonl';
export const PDS_HEALTH_CHECK_CONCURRENCY = 20;
export const PDS_DATA_FETCH_CONCURRENCY = 150;
export const DIDS_TO_PROCESS = parseInt(process.argv[2], 10) || 10000;
export const BATCH_SIZE = 5000; // Number of records per batch
export const BATCH_TIMEOUT_MS = 1000; // 1 second
export const MAX_FLUSH_RETRIES = 5; // Maximum number of retry attempts for flushing
