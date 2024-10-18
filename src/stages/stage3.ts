import { Mutex } from 'async-mutex';
import axios from 'axios';
import emojiRegexFunc from 'emoji-regex';
import fs from 'fs';
import pLimit from 'p-limit';

import { PDS_DATA_FETCH_CONCURRENCY, PYTHON_SERVICE_TIMEOUT_MS, SUCCESSFUL_DIDS_LOG_INTERVAL } from '../constants.js';
import { postBatchQueue, profileBatchQueue } from '../db/postgresBatchQueues.js';
import { batchNormalizeEmojis } from '../emojiNormalization.js';
import { chunkArray } from '../helpers/generic.js';
import { sanitizeString, sanitizeTimestamp } from '../helpers/sanitize.js';
import {
  concurrentRedisInserts,
  didsConcurrentProcessing,
  didsFailedTotal,
  didsProcessedTotal,
  didsProcessingDuration,
  didsRetryTotal,
  didsSuccessfulTotal,
  totalDescriptionNameEmojis,
  totalDescriptionNamesWithEmojis,
  totalDescriptionNamesWithoutEmojis,
  totalDisplayNameEmojis,
  totalDisplayNamesWithEmojis,
  totalDisplayNamesWithoutEmojis,
  totalEmojis,
  totalPostsWithEmojis,
  totalPostsWithoutEmojis,
  totalProcessedPosts,
  totalProcessedProfiles,
} from '../metrics.js';
import {
  DESCRIPTION_NAMES_WITHOUT_EMOJIS,
  DESCRIPTION_SCRIPT,
  DISPLAY_NAMES_WITHOUT_EMOJIS,
  DISPLAY_NAME_SCRIPT,
  POSTS_WITHOUT_EMOJIS,
  POST_SCRIPT,
  PROCESSED_POSTS,
  PROCESSED_PROFILES,
  redis,
} from '../redis.js';
import {
  BskyData,
  BskyPost,
  BskyPostData,
  BskyProfile,
  BskyProfileData,
  DidAndPds,
  DidProcessingStatus,
  PostData,
  ProfileData,
} from '../types.js';

const emojiRegex: RegExp = emojiRegexFunc();

// Initialize mutex for writing to the weird timestamps file
const writeMutex = new Mutex();

// Define the path for the weird timestamps log file
const weirdTimestampsFilePath = 'weird_timestamps.log';

// Create a write stream for the weird timestamps file in append mode
const weirdTimestampsStream = fs.createWriteStream(weirdTimestampsFilePath, { flags: 'a' });

interface ProcessingContext {
  successfulDids: number;
  retryDids: number;
  successfulRequests: number;
  unsuccessfulRequests: number;
  failedDids: number;
}

// Function to log weird timestamps safely
async function logWeirdTimestamp(details: {
  did: string;
  rkey: string;
  cid: string;
  originalCreatedAt: string;
  sanitizedCreatedAt: string;
  type: 'post' | 'profile';
}): Promise<void> {
  const logEntry = JSON.stringify(details) + '\n';
  await writeMutex.runExclusive(() => {
    return new Promise<void>((resolve, reject) => {
      weirdTimestampsStream.write(logEntry, (err) => {
        if (err) {
          console.error(`Failed to write to weird timestamps file: ${err.message}`);
          reject(err);
        } else {
          resolve();
        }
      });
    });
  });
}

function processLanguages(langs?: string[]): Set<string> {
  const languageSet = new Set<string>();
  if (Array.isArray(langs) && langs.length > 0) {
    return new Set(langs);
  }
  languageSet.add('unknown');
  return languageSet;
}

function extractEmojis(text: string | undefined | null): { hasEmojis: boolean; normalizedEmojis: string[] } {
  let hasEmojis = false;
  let emojiMatches: RegExpMatchArray | [] | undefined = [];
  let normalizedEmojis: string[] = [];
  if (typeof text === 'string') {
    emojiMatches = text.match(emojiRegex) ?? [];
    normalizedEmojis = batchNormalizeEmojis(emojiMatches);
    hasEmojis = normalizedEmojis.length > 0;
  }
  return { hasEmojis, normalizedEmojis };
}

async function processPost(key: string, value: unknown, did: string): Promise<void> {
  const post = value as BskyPost;
  const postData = post.value as unknown as BskyPostData;
  let rkey = sanitizeString(key.split('/').pop());
  // This is probably too paranoid but you never know with Bluesky
  if (rkey === '') {
    rkey = Math.random().toString(36).substring(2, 15);
  }
  const { timestamp, wasWeird, defaulted } = sanitizeTimestamp(postData.createdAt);

  if (wasWeird) {
    console.error(`Weird post timestamp for DID: ${did}
      rkey: ${rkey}
      cid: ${postData.cid}
      original createdAt: ${postData.createdAt}
      sanitized createdAt: ${timestamp}`);

    // Log the weird timestamp to the file
    await logWeirdTimestamp({
      did,
      rkey: sanitizeString(rkey),
      cid: postData.cid,
      originalCreatedAt: postData.createdAt,
      sanitizedCreatedAt: timestamp,
      type: 'post',
    });
  }

  const langs = processLanguages(postData.langs);
  const text = sanitizeString(postData.text);
  const { hasEmojis, normalizedEmojis } = extractEmojis(text);

  const data: PostData = {
    cid: postData.cid,
    did,
    rkey: sanitizeString(rkey),
    hasEmojis,
    langs: Array.from(langs),
    emojis: normalizedEmojis,
    post: text,
    createdAt: timestamp,
  };

  try {
    await postBatchQueue.enqueue(data);
  } catch (err: unknown) {
    console.error(`Post data enqueue error for DID ${did}: ${(err as Error).message}`);
    await redis.set(`${did}:status`, 'retry');
    throw err;
  }

  /* step 2: redis */
  concurrentRedisInserts.inc();
  if (!hasEmojis) {
    await redis.incr(POSTS_WITHOUT_EMOJIS);
    totalPostsWithoutEmojis.inc();
  } else {
    await redis.evalSha(POST_SCRIPT, {
      arguments: [JSON.stringify(normalizedEmojis), JSON.stringify(Array.from(langs))],
    });

    totalEmojis.inc(normalizedEmojis.length);
    totalPostsWithEmojis.inc();
  }

  /* step 3: global metrics */
  await redis.incr(PROCESSED_POSTS);
  totalProcessedPosts.inc();
  concurrentRedisInserts.dec();
}

async function processProfile(key: string, value: unknown, did: string): Promise<void> {
  const profile = value as BskyProfile;
  const profileData = profile.value as unknown as BskyProfileData;
  let rkey = sanitizeString(key.split('/').pop());
  // This is probably too paranoid but you never know with Bluesky
  if (rkey === '') {
    rkey = Math.random().toString(36).substring(2, 15);
  }
  const { timestamp, wasWeird, defaulted } = sanitizeTimestamp(profileData.createdAt);

  if (wasWeird) {
    console.error(`Weird profile timestamp for DID: ${did}
      rkey: ${rkey}
      cid: ${profileData.cid}
      createdAt: ${timestamp}`);

    // Log the weird timestamp to the file
    await logWeirdTimestamp({
      did,
      rkey: sanitizeString(rkey),
      cid: profileData.cid,
      originalCreatedAt: profileData.createdAt,
      sanitizedCreatedAt: timestamp,
      type: 'profile',
    });
  }

  const { hasEmojis: hasDisplayNameEmojis, normalizedEmojis: normalizedDisplayNameEmojis } = extractEmojis(
    profileData.displayName,
  );
  const { hasEmojis: hasDescriptionEmojis, normalizedEmojis: normalizedDescriptionEmojis } = extractEmojis(
    profileData.description,
  );

  const data: ProfileData = {
    cid: profileData.cid,
    did,
    rkey,
    displayName: sanitizeString(profileData.displayName),
    description: sanitizeString(profileData.description),
    createdAt: timestamp,
    hasDisplayNameEmojis,
    hasDescriptionEmojis,
    displayNameEmojis: normalizedDisplayNameEmojis,
    descriptionEmojis: normalizedDescriptionEmojis,
  };

  try {
    await profileBatchQueue.enqueue(data);
  } catch (err: unknown) {
    console.error(`Profile data enqueue error for DID ${did}: ${(err as Error).message}`);
    await redis.set(`${did}:status`, 'retry');
    throw err;
  }

  concurrentRedisInserts.inc();
  if (!hasDisplayNameEmojis) {
    await redis.incr(DISPLAY_NAMES_WITHOUT_EMOJIS);
    totalDisplayNamesWithoutEmojis.inc();
  } else {
    await redis.evalSha(DISPLAY_NAME_SCRIPT, {
      arguments: [JSON.stringify(normalizedDisplayNameEmojis)],
    });

    totalDisplayNameEmojis.inc(normalizedDisplayNameEmojis.length);
    totalDisplayNamesWithEmojis.inc();
  }

  if (!hasDescriptionEmojis) {
    await redis.incr(DESCRIPTION_NAMES_WITHOUT_EMOJIS);
    totalDescriptionNamesWithoutEmojis.inc();
  } else {
    await redis.evalSha(DESCRIPTION_SCRIPT, {
      arguments: [JSON.stringify(normalizedDescriptionEmojis)],
    });

    totalDescriptionNameEmojis.inc(normalizedDescriptionEmojis.length);
    totalDescriptionNamesWithEmojis.inc();
  }

  /* step 3: global metrics */
  await redis.incr(PROCESSED_PROFILES);
  totalProcessedProfiles.inc();
  concurrentRedisInserts.dec();
}

export async function processDidsAndFetchData(dids: DidAndPds[]): Promise<void> {
  const limit = pLimit(PDS_DATA_FETCH_CONCURRENCY);
  const context: ProcessingContext = {
    successfulRequests: 0,
    unsuccessfulRequests: 0,
    successfulDids: 0,
    failedDids: 0,
    retryDids: 0,
  };

  const tasks = dids.map(({ did, pds }) =>
    limit(async () => {
      didsConcurrentProcessing.inc();
      const endTimer = didsProcessingDuration.startTimer();
      try {
        const status: DidProcessingStatus = (await redis.get(`${did}:status`)) as DidProcessingStatus;

        if (status === 'completed' || status === 'failed') return;

        if (status === 'retry' || status === 'processing') {
          console.log(`Retrying DID set to ${status}: ${did}`);
        }

        await redis.set(`${did}:status`, 'processing');

        try {
          const res = await axios.post(
            'http://localhost:8000/fetch',
            { did, pds },
            {
              responseType: 'stream',
              timeout: PYTHON_SERVICE_TIMEOUT_MS,
            },
          );

          context.successfulRequests++;
          didsSuccessfulTotal.inc();
          didsProcessedTotal.inc();

          // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
          await processStream(res.data, did, context);
        } catch (error) {
          if (!(error as Error).message.includes('Request failed with status code 502')) {
            console.error(`Error with DID ${did}: ${(error as Error).message}`);
          }

          try {
            await redis.set(`${did}:status`, 'failed');
          } catch (redisError: unknown) {
            console.error(`Redis set error for DID ${did}: ${(redisError as Error).message}`);
          }

          context.unsuccessfulRequests++;
          context.failedDids++;
          didsFailedTotal.inc();
        }
      } finally {
        endTimer();
        didsConcurrentProcessing.dec();
      }
    }),
  );

  const chunkedTasks = chunkArray(tasks, 10000);
  for (const chunk of chunkedTasks) {
    try {
      await Promise.all(chunk);
      console.log(`Processed ${chunk.length} tasks.`);
    } catch (err: unknown) {
      console.error(`Error processing a chunk of tasks: ${(err as Error).message}`);
    }
  }

  console.log(`Processed DIDs.`);
  console.log(
    `Successful requests: ${context.successfulRequests}, Unsuccessful requests: ${context.unsuccessfulRequests}`,
  );
  console.log(
    `Successful DIDs: ${context.successfulDids}, Failed DIDs: ${context.failedDids}, Retry DIDs: ${context.retryDids}`,
  );
}

async function processStream(stream: NodeJS.ReadableStream, did: string, context: ProcessingContext): Promise<void> {
  let buffer = '';

  try {
    for await (const chunk of stream) {
      buffer += chunk.toString();
      let boundary = buffer.indexOf('\n');

      while (boundary !== -1) {
        const line = buffer.substring(0, boundary);
        buffer = buffer.substring(boundary + 1);

        if (line.trim()) {
          try {
            const json = JSON.parse(line) as BskyData;
            await processLine(json, did);
          } catch (err: unknown) {
            console.error(`JSON parse error for DID ${did}: ${(err as Error).message}`);
          }
        }

        boundary = buffer.indexOf('\n');
      }
    }

    if (buffer.trim()) {
      try {
        // we should never get here
        const json = JSON.parse(buffer) as BskyData;
        console.dir(json, { depth: null });
        if (Object.keys(json).length > 0) {
          console.error('JSON is not empty', json);
          console.log(did);
          throw new Error('JSON is not empty');
        }
      } catch (err: unknown) {
        console.error(`JSON parse error at stream end for DID ${did}: ${(err as Error).message}`);
      }
    }

    const status: DidProcessingStatus = (await redis.get(`${did}:status`)) as DidProcessingStatus;
    if (status !== 'retry') {
      await redis.set(`${did}:status`, 'completed');
      context.successfulDids++;
      didsProcessedTotal.inc();

      if (context.successfulDids % SUCCESSFUL_DIDS_LOG_INTERVAL === 0) {
        console.log(`Processed ${context.successfulDids} DIDs.`);
      }
    }
  } catch (err: unknown) {
    console.error(`Stream error for DID ${did}: ${(err as Error).message}`);

    try {
      await redis.set(`${did}:status`, 'retry');
      context.retryDids++;
      didsRetryTotal.inc();
    } catch (redisError: unknown) {
      console.error(`Redis set error for DID ${did}: ${(redisError as Error).message}`);
    }

    throw err;
  }
}

async function processLine(json: BskyData, did: string): Promise<void> {
  const processingTasks: Promise<void>[] = [];

  for (const [key, value] of Object.entries(json)) {
    if (key.includes('app.bsky.feed.post')) {
      processingTasks.push(processPost(key, value, did));
    } else if (key.includes('app.bsky.actor.profile')) {
      processingTasks.push(processProfile(key, value, did));
    }
  }

  await Promise.all(processingTasks);
}

// Export a shutdown function to close the weird timestamps write stream
export async function shutdownStage3(): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    writeMutex
      .runExclusive(() => {
        return new Promise<void>((innerResolve, innerReject) => {
          weirdTimestampsStream.end(() => {
            console.log('Weird timestamps write stream closed.');
            innerResolve();
          });
          weirdTimestampsStream.on('error', (err) => {
            console.error(`Error closing weird timestamps write stream: ${err.message}`);
            innerReject(err);
          });
        });
      })
      .then(resolve)
      .catch(reject);
  });
}
