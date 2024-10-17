import { createClient } from 'redis';

import { REDIS_URL } from './constants.js';

const redis = createClient({ url: REDIS_URL });

redis.on('error', (err: Error) => {
  console.error('Redis Client Error', { error: err });
});

redis.on('connect', () => {
  console.info('Connected to Redis.');
});

redis.on('ready', () => {
  console.info('Redis client ready.');
});

redis.on('end', () => {
  console.info('Redis client disconnected.');
});

// let SCRIPT_SHA: string;

// const loadRedisScripts = async () => {
//   const scriptPath = new URL('lua/incrementEmojis.lua', import.meta.url);
//   const incrementEmojisScript = fs.readFileSync(scriptPath, 'utf8');
//   SCRIPT_SHA = await redis.scriptLoad(incrementEmojisScript);
//   console.info(`Loaded Redis script with SHA: ${SCRIPT_SHA}`);
// };

// export { redis, loadRedisScripts, SCRIPT_SHA };

export { redis };
