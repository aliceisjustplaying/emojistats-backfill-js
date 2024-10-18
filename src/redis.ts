import fs from 'fs';
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

let POST_SCRIPT: string;
let DISPLAY_NAME_SCRIPT: string;
let DESCRIPTION_SCRIPT: string;

const loadRedisScripts = async () => {
  const scriptPath = new URL('lua/incrementPostEmojis.lua', import.meta.url);
  const incrementEmojisScript = fs.readFileSync(scriptPath, 'utf8');
  POST_SCRIPT = await redis.scriptLoad(incrementEmojisScript);
  const displayNameScriptPath = new URL('lua/incrementDisplayNameEmojis.lua', import.meta.url);
  const displayNameScript = fs.readFileSync(displayNameScriptPath, 'utf8');
  DISPLAY_NAME_SCRIPT = await redis.scriptLoad(displayNameScript);
  const descriptionScriptPath = new URL('lua/incrementDescriptionEmojis.lua', import.meta.url);
  const descriptionScript = fs.readFileSync(descriptionScriptPath, 'utf8');
  DESCRIPTION_SCRIPT = await redis.scriptLoad(descriptionScript);
  console.info(`Loaded Post script with SHA: ${POST_SCRIPT}`);
  console.info(`Loaded Display Name script with SHA: ${DISPLAY_NAME_SCRIPT}`);
  console.info(`Loaded Description script with SHA: ${DESCRIPTION_SCRIPT}`);
};

const EMOJI_SORTED_SET = 'emojiStats';
const LANGUAGE_SORTED_SET = 'languageStats';
const PROCESSED_POSTS = 'processedPosts';
const POSTS_WITH_EMOJIS = 'postsWithEmojis';
const POSTS_WITHOUT_EMOJIS = 'postsWithoutEmojis';
const PROCESSED_EMOJIS = 'processedEmojis';

const DISPLAY_NAMES_WITHOUT_EMOJIS = 'displayNamesWithoutEmojis';
const DISPLAY_NAMES_WITH_EMOJIS = 'displayNamesWithEmojis';
const DESCRIPTION_NAMES_WITHOUT_EMOJIS = 'descriptionNamesWithoutEmojis';
const DESCRIPTION_NAMES_WITH_EMOJIS = 'descriptionNamesWithEmojis';
const PROCESSED_PROFILES = 'processedProfiles';

export {
  redis,
  loadRedisScripts,
  POST_SCRIPT,
  DISPLAY_NAME_SCRIPT,
  DESCRIPTION_SCRIPT,
  EMOJI_SORTED_SET,
  LANGUAGE_SORTED_SET,
  PROCESSED_POSTS,
  POSTS_WITH_EMOJIS,
  POSTS_WITHOUT_EMOJIS,
  PROCESSED_EMOJIS,
  DISPLAY_NAMES_WITHOUT_EMOJIS,
  DISPLAY_NAMES_WITH_EMOJIS,
  DESCRIPTION_NAMES_WITHOUT_EMOJIS,
  DESCRIPTION_NAMES_WITH_EMOJIS,
  PROCESSED_PROFILES,
};
