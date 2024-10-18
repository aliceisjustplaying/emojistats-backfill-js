import axios from 'axios';
import emojiRegexFunc from 'emoji-regex';
import pLimit from 'p-limit';

import { PDS_DATA_FETCH_CONCURRENCY, PYTHON_SERVICE_TIMEOUT_MS, SUCCESSFUL_DIDS_LOG_INTERVAL } from '../constants.js';
import { postBatchQueue, profileBatchQueue } from '../db/postgresBatchQueues.js';
import { batchNormalizeEmojis } from '../emojiNormalization.js';
import { chunkArray, sanitizeString, sanitizeTimestamp } from '../helpers.js';
import { redis } from '../redis.js';
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

export async function processDidsAndFetchData(dids: DidAndPds[]): Promise<void> {
  const limit = pLimit(PDS_DATA_FETCH_CONCURRENCY);
  let successfulRequests = 0;
  let unsuccessfulRequests = 0;
  let successfulDids = 0;
  let failedDids = 0;
  let retryDids = 0;

  const tasks = dids.map(({ did, pds }) =>
    limit(async () => {
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

        successfulRequests++;

        await new Promise<void>((resolve, reject) => {
          let buffer = '';

          // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
          res.data.on('data', (chunk: Buffer) => {
            buffer += chunk.toString();
            let boundary = buffer.indexOf('\n');
            while (boundary !== -1) {
              const line = buffer.substring(0, boundary);
              buffer = buffer.substring(boundary + 1);
              if (line.trim()) {
                try {
                  const json = JSON.parse(line) as BskyData;
                  for (const [k, v] of Object.entries(json)) {
                    if (k.includes('app.bsky.feed.post')) {
                      const post = v as BskyPost;
                      const postData = post.value as unknown as BskyPostData;
                      const rkeyParts = k.split('/');
                      const rkey = rkeyParts.length > 1 ? rkeyParts[1] : k;
                      let timestamp = '';
                      let isValid = false;
                      if (!postData.createdAt) {
                        timestamp = '1970-01-01T00:00:00.000Z';
                      } else {
                        const result = sanitizeTimestamp(postData.createdAt);
                        timestamp = result.timestamp;
                        isValid = result.isValid;
                        if (!isValid) {
                          console.log(`DID: ${did}`);
                          console.log(`rkey: ${rkey}`);
                          console.log(`cid: ${postData.cid}`);
                          console.error(`Invalid timestamp: ${postData.createdAt}`);
                          timestamp = '1970-01-01T00:00:00.000Z';
                        }
                      }

                      if (typeof timestamp === 'undefined') {
                        console.error(`Timestamp is undefined for DID: ${did}`);
                        console.error(`rkey: ${rkey}`);
                        console.error(`cid: ${postData.cid}`);
                        timestamp = '1970-01-01T00:00:00.000Z';
                      }

                      let langs = new Set<string>();
                      if (Array.isArray(postData.langs) && postData.langs.length > 0) {
                        langs = new Set(postData.langs);
                      } else {
                        langs.add('unknown');
                      }

                      let hasEmojis = false;
                      let emojiMatches: RegExpMatchArray | [] | undefined = [];
                      let normalizedEmojis: string[] = [];
                      if (postData.text && typeof postData.text === 'string') {
                        emojiMatches = postData.text.match(emojiRegex) ?? [];
                        normalizedEmojis = batchNormalizeEmojis(emojiMatches);
                        hasEmojis = normalizedEmojis.length > 0;
                      } else {
                        postData.text = '';
                      }

                      const data: PostData = {
                        cid: postData.cid,
                        did: did,
                        rkey: sanitizeString(rkey),
                        hasEmojis: hasEmojis,
                        langs: Array.from(langs),
                        emojis: normalizedEmojis,
                        post: sanitizeString(postData.text),
                        createdAt: timestamp,
                      };
                      postBatchQueue.enqueue(data).catch((err: unknown) => {
                        console.error(`Post data enqueue error for DID ${did}: ${(err as Error).message}`);
                        redis
                          .set(`${did}:status`, 'retry')
                          .then(() => {
                            retryDids++;
                            resolve();
                          })
                          .catch((err: unknown) => {
                            console.log('AAAAAAAAAAAAAAAAAAAAAAAA');
                            console.error(`Redis set error for DID ${did}: ${(err as Error).message}`);
                            reject(err as Error);
                          });
                      });
                    } else if (k.includes('app.bsky.actor.profile')) {
                      const profile = v as BskyProfile;
                      const profileData = profile.value as unknown as BskyProfileData;
                      const rkeyParts = k.split('/');
                      const rkey = rkeyParts.length > 1 ? rkeyParts[1] : k;
                      let timestamp = '';
                      let isValid = false;
                      if (!profileData.createdAt) {
                        timestamp = '1970-01-01T00:00:00.000Z';
                      } else {
                        const result = sanitizeTimestamp(profileData.createdAt);
                        timestamp = result.timestamp;
                        isValid = result.isValid;
                        if (!isValid) {
                          console.log(`DID: ${did}`);
                          console.log(`rkey: ${rkey}`);
                          console.log(`cid: ${profileData.cid}`);
                          console.error(`Invalid timestamp: ${profileData.createdAt}`);
                          timestamp = '1970-01-01T00:00:00.000Z';
                        }
                      }

                      if (typeof timestamp === 'undefined') {
                        console.error(`Timestamp is undefined for DID: ${did}`);
                        console.error(`rkey: ${rkey}`);
                        console.error(`cid: ${profileData.cid}`);
                        timestamp = '1970-01-01T00:00:00.000Z';
                      }

                      const displayNameEmojiMatches = profileData.displayName?.match(emojiRegex) ?? [];
                      const descriptionEmojiMatches = profileData.description?.match(emojiRegex) ?? [];
                      const normalizedDisplayNameEmojis = batchNormalizeEmojis(displayNameEmojiMatches);
                      const normalizedDescriptionEmojis = batchNormalizeEmojis(descriptionEmojiMatches);
                      const hasDisplayNameEmojis = normalizedDisplayNameEmojis.length > 0;
                      const hasDescriptionEmojis = normalizedDescriptionEmojis.length > 0;

                      const data: ProfileData = {
                        cid: profileData.cid,
                        did: did,
                        rkey: sanitizeString(rkey),
                        displayName: sanitizeString(profileData.displayName ?? ''),
                        description: sanitizeString(profileData.description ?? ''),
                        createdAt: timestamp,
                        hasDisplayNameEmojis: hasDisplayNameEmojis,
                        hasDescriptionEmojis: hasDescriptionEmojis,
                        displayNameEmojis: normalizedDisplayNameEmojis,
                        descriptionEmojis: normalizedDescriptionEmojis,
                      };

                      profileBatchQueue.enqueue(data).catch((err: unknown) => {
                        console.error(`Profile data enqueue error for DID ${did}: ${(err as Error).message}`);
                        redis
                          .set(`${did}:status`, 'retry')
                          .then(() => {
                            retryDids++;
                            resolve();
                          })
                          .catch((err: unknown) => {
                            console.log('AAAAAAAAAAAAAAAAAAAAAAAA');
                            console.error(`Redis set error for DID ${did}: ${(err as Error).message}`);
                            reject(err as Error);
                          });
                      });
                    }
                  }
                } catch (err) {
                  console.error(`JSON parse error for DID ${did}: ${(err as Error).message}`);
                }
              }
              boundary = buffer.indexOf('\n');
            }
          });

          // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
          res.data.on('end', () => {
            if (buffer.trim()) {
              try {
                const json = JSON.parse(buffer) as BskyData;
                console.dir(json, { depth: null });
                if (Object.keys(json).length > 0) {
                  throw new Error('JSON is not empty');
                }
              } catch (err) {
                console.error(`JSON parse error at stream end for DID ${did}: ${(err as Error).message}`);
              }
            }
            redis
              .get(`${did}:status`)
              .then((status) => {
                if (status === 'retry') {
                  return;
                }
                redis
                  .set(`${did}:status`, 'completed')
                  .then(() => {
                    successfulDids++;
                    if (successfulDids % SUCCESSFUL_DIDS_LOG_INTERVAL === 0) {
                      // process.stdout.write('#');
                      console.log(`Processed ${successfulDids} DIDs.`);
                    }
                    resolve();
                  })
                  .catch((err: unknown) => {
                    console.error(`Redis set error for DID ${did}: ${(err as Error).message}`);
                  });
              })
              .catch((err: unknown) => {
                console.error(`Redis get error for DID ${did}: ${(err as Error).message}`);
              });
          });

          // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
          res.data.on('error', (err: Error) => {
            console.error(`Stream error for DID ${did}: ${err.message}`);

            // I *think* retry is the reasonable thing to do here, since the request failed
            // so something on the Python side is wrong. Maybe? lol.
            redis.set(`${did}:status`, 'retry').catch((err: unknown) => {
              console.error(`Redis set error for DID ${did}: ${(err as Error).message}`);
            });
            retryDids++;
            reject(err);
          });
        });
      } catch (error) {
        // this happens when the user doesn't exist anymore, usually
        if (!(error as Error).message.includes('Request failed with status code 502')) {
          console.error(`Error with DID ${did}: ${(error as Error).message}`);
        }

        redis.set(`${did}:status`, 'failed').catch((err: unknown) => {
          console.error(`Redis set error for DID ${did}: ${(err as Error).message}`);
        });

        unsuccessfulRequests++;
        failedDids++;
      }
    }),
  );

  const chunkedTasks = chunkArray(tasks, 10000);
  for (const chunk of chunkedTasks) {
    await Promise.all(chunk);
    console.log(`Processed ${chunk.length} tasks.`);
  }
  console.log(`Processed DIDs.`);
  console.log(`Successful requests: ${successfulRequests}, Unsuccessful requests: ${unsuccessfulRequests}`);
  console.log(`Successful DIDs: ${successfulDids}, Failed DIDs: ${failedDids}, Retry DIDs: ${retryDids}`);
}
