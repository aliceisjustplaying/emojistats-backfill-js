export const RELAY_URL = process.env.RELAY_URL!;
export const PLC_DB_PATH = process.env.PLC_DB_PATH!;
export const SQL_OUTPUT_FILE = 'dids_pds.jsonl';
export const HEALTH_CHECK_FILE = 'pds_health.json';
export const DATA_OUTPUT_FILE = 'bsky_data.jsonl';
export const PDS_HEALTH_CHECK_CONCURRENCY = 20;
export const PDS_DATA_FETCH_CONCURRENCY = 100;
export const DIDS_TO_PROCESS = 20000;
