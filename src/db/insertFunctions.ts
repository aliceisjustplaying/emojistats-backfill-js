import { PostData, ProfileData } from '../types.js';
import { db } from './postgres.js';

export const insertPosts = async (batch: PostData[]): Promise<void> => {
  await db.transaction().execute(async (tx) => {
    await tx
      .insertInto('posts')
      .values(
        batch.map((post) => ({
          cid: post.cid,
          did: post.did,
          rkey: post.rkey,
          has_emojis: post.hasEmojis,
          langs: post.langs,
          text: post.post,
          created_at: new Date(post.createdAt),
        })),
      )
      .execute();
  });
};

export const insertProfiles = async (batch: ProfileData[]): Promise<void> => {
  await db.transaction().execute(async (tx) => {
    await tx
      .insertInto('profiles')
      .values(
        batch.map((profile) => ({
          cid: profile.cid,
          did: profile.did,
          rkey: profile.rkey,
          display_name: profile.displayName,
          description: profile.description,
          created_at: new Date(profile.createdAt),
        })),
      )
      .execute();
  });
};
