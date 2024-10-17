import axios from 'axios';
import pLimit from 'p-limit';

import { PDS_DATA_FETCH_CONCURRENCY } from '../constants.js';
import { sanitizeTimestamp } from '../helpers.js';
import { postgresBatchQueue } from '../postgresBatchQueue.js';
import { BskyData, BskyPost, BskyPostData, DidAndPds, PostData } from '../types.js';

export async function processDidsAndFetchData(dids: DidAndPds[]): Promise<void> {
  const limit = pLimit(PDS_DATA_FETCH_CONCURRENCY);
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
            timeout: 30 * 60 * 1000, // 30 minutes
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
                      const sanitizedCreatedAt = sanitizeTimestamp(postData.createdAt);
                      const data: PostData = {
                        cid: postData.cid,
                        did: did,
                        rkey: rkey,
                        hasEmojis: false,
                        langs: postData.langs,
                        post: postData.text,
                        createdAt: sanitizedCreatedAt,
                      };
                      postgresBatchQueue.enqueue(data).catch((err: unknown) => {
                        console.error(`Enqueue error for DID ${did}: ${(err as Error).message}`);
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
            successfulDids++;
            if (successfulDids % 100 === 0) {
              process.stdout.write('#');
            }
            resolve();
          });

          // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
          res.data.on('error', (err: Error) => {
            console.error(`Stream error for DID ${did}: ${err.message}`);
            failedDids++;
            if (failedDids % 100 === 0) {
              process.stdout.write('*');
            }
            reject(err);
          });
        });
      } catch (error) {
        if (!(error as Error).message.includes('Request failed with status code 502')) {
          console.error(`Error with DID ${did}: ${(error as Error).message}`);
        }
        unsuccessfulRequests++;
        failedDids++;
      }
    }),
  );

  await Promise.all(tasks);
  console.log(`Processed DIDs.`);
  console.log(`Successful requests: ${successfulRequests}, Unsuccessful requests: ${unsuccessfulRequests}`);
  console.log(`Successful DIDs: ${successfulDids}, Failed DIDs: ${failedDids}`);
}
