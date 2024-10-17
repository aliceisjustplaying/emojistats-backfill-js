import axios from 'axios';
import pLimit from 'p-limit';

import { PDS_DATA_FETCH_CONCURRENCY } from '../constants.js';
import logger from '../logger.js';
import { db } from '../postgres.js';
import { BskyData, BskyPost, BskyPostData } from '../types.js';

const BATCH_SIZE = 1000;
const BATCH_TIMEOUT_MS = 1000;

interface PostData {
  cid: string;
  did: string;
  rkey: string;
  hasEmojis: boolean;
  langs: string[];
  post: string;
  createdAt: string;
}

let postBatch: PostData[] = [];
let postBatchCount = 0;
let isBatching = false;
let batchTimer: NodeJS.Timeout | null = null;

let isShuttingDown = false;
const ongoingHandleCreates = 0;
let shutdownPromise: Promise<void> | null = null;

function createShutdownPromise(): Promise<void> {
  return new Promise<void>((resolve) => {
    const checkCompletion = setInterval(() => {
      console.log(`Shutting down, ongoing handleCreates: ${ongoingHandleCreates}`);
      if (isShuttingDown && ongoingHandleCreates === 0) {
        console.log('All ongoing handleCreate operations have finished.');
        clearInterval(checkCompletion);
        resolve();
      }
    }, 50);
  });
}

export function initiateShutdown(): Promise<void> {
  if (!shutdownPromise) {
    isShuttingDown = true;
    shutdownPromise = createShutdownPromise();
  }
  return shutdownPromise;
}

export async function flushPostgresBatch() {
  if (postBatch.length === 0) {
    isBatching = false;
    return;
  }

  const currentBatch = [...postBatch];
  postBatch = [];
  isBatching = false;

  // concurrentPostgresInserts.inc();

  try {
    await db.transaction().execute(async (tx) => {
      // Bulk insert posts
      const insertedPosts = await tx
        .insertInto('posts')
        .values(
          currentBatch.map((post) => ({
            cid: post.cid,
            did: post.did,
            rkey: post.rkey,
            has_emojis: post.hasEmojis,
            langs: post.langs,
            text: post.post,
            created_at: post.createdAt,
          })),
        )
        // .returning(['id', 'cid', 'did', 'rkey'])
        .execute();

      //   // Map composite key to id
      //   const compositeKeyToIdMap = new Map<string, number>();
      //   insertedPosts.forEach((post) => {
      //     const compositeKey = `${post.cid}-${post.did}-${post.rkey}`;
      //     compositeKeyToIdMap.set(compositeKey, post.id);
      //   });

      //   // Prepare bulk insert for emojis
      //   const emojiInserts: { post_id: number; emoji: string; lang: string }[] = [];
      //   currentBatch.forEach((post) => {
      //     if (post.hasEmojis) {
      //       const compositeKey = `${post.cid}-${post.did}-${post.rkey}`;
      //       const postId = compositeKeyToIdMap.get(compositeKey);
      //       if (postId) {
      //         post.emojis.forEach((emoji) => {
      //           post.langs.forEach((lang) => {
      //             emojiInserts.push({
      //               post_id: postId,
      //               emoji: emoji,
      //               lang: lang,
      //             });
      //           });
      //         });
      //       }
      //     }
      //   });

      //   if (emojiInserts.length > 0) {
      //     await tx.insertInto('emojis').values(emojiInserts).execute();
      //   }
    });

    // concurrentPostgresInserts.dec();
  } catch (error) {
    console.error(`Error flushing PostgreSQL batch: ${(error as Error).message}`);
    // Optionally, you can re-add the failed batch back to `postBatch` for retry
    postBatch = currentBatch.concat(postBatch);
  }
}

/**
 * Schedule a batch flush after a timeout.
 */
function scheduleBatchFlush() {
  if (batchTimer) {
    return;
  }
  batchTimer = setTimeout(() => {
    batchTimer = null;
    void flushPostgresBatch();
  }, BATCH_TIMEOUT_MS);
}

export async function processDidsAndFetchData(dids: { did: string; pds: string }[]) {
  const limit = pLimit(PDS_DATA_FETCH_CONCURRENCY);
  const fetchedData: BskyData[] = [];
  let successfulRequests = 0;
  let unsuccessfulRequests = 0;
  let successfulDids = 0;
  let failedDids = 0;

  const tasks = dids.map(({ did, pds }) =>
    limit(async () => {
      try {
        const res = await axios.post(
          'http://localhost:8000/fetch',
          { did, pds },
          {
            responseType: 'stream',
            timeout: 60000,
          },
        );

        successfulRequests++;

        await new Promise<void>((resolve, reject) => {
          let buffer = '';
          let didSucceeded = false;

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
                  postBatchCount++;
                  if (postBatchCount % 1000 === 0) {
                    process.stdout.write('.');
                    void flushPostgresBatch();
                  }
                  // console.log('json', json);
                  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
                  if (json) {
                    for (const [k, v] of Object.entries(json)) {
                      if (k.includes('app.bsky.feed.post')) {
                        const post = v as BskyPost;
                        const postData = (post.value as unknown) as BskyPostData;
                        postBatch.push({
                          cid: postData.cid,
                          did: did,
                          rkey: k.split('/')[1],
                          hasEmojis: false,
                          langs: postData.langs,
                          post: postData.text,
                          createdAt: postData.createdAt,
                        });
                      }
                    }
                  }
                  didSucceeded = true;
                } catch (err) {
                  console.error(`JSON parse error for DID ${did}: ${(err as Error).message}`);
                }
              }
              boundary = buffer.indexOf('\n');
            }
          });

          // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
          res.data.on('end', () => {
            scheduleBatchFlush();
            if (buffer.trim()) {
              try {
                const json = JSON.parse(buffer) as BskyData;
                console.log('OMG WE GOT HERE OOPS NEED TO HANDLE THIS');
                console.log(json);
                didSucceeded = true;
              } catch (err) {
                console.error(`JSON parse error at stream end for DID ${did}: ${(err as Error).message}`);
              }
            }
            if (didSucceeded) {
              successfulDids++;
            } else {
              failedDids++;
            }
            resolve();
          });

          // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
          res.data.on('error', (err: Error) => {
            console.error(`Stream error for DID ${did}: ${err.message}`);
            failedDids++;
            reject(err);
          });
        });
        return;
      } catch (error) {
        process.stdout.write('!');
        // this just means the user doesn't exist anymore for whatever reason
        if (!(error as Error).message.includes('Request failed with status code 502')) {
          console.error(`Error with DID ${did}: ${(error as Error).message}`);
        }
        unsuccessfulRequests++;
        failedDids++;
      }
    }),
  );

  await Promise.all(tasks);
  console.log(`Fetched data for ${fetchedData.length} DIDs.`);
  console.log(`Successful requests: ${successfulRequests}, Unsuccessful requests: ${unsuccessfulRequests}`);
  console.log(`Successful DIDs: ${successfulDids}, Failed DIDs: ${failedDids}`);

  // const writeFile = await fs.open(DATA_OUTPUT_FILE, 'w');
  // const writeStream = writeFile.createWriteStream();
  // for (const data of fetchedData) {
  //   writeStream.write(JSON.stringify(data) + '\n');
  // }
  // writeStream.close();
  // console.log(`Streamed fetched data to ${DATA_OUTPUT_FILE}`);
  return fetchedData;
}
