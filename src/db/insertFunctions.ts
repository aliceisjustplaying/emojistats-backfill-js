import { EMOJI_BATCH_SIZE } from '../constants.js';
import { chunkArray } from '../helpers.js';
import type { PostData, ProfileData } from '../types.js';
import { db } from './postgres.js';

export const insertPosts = async (batch: PostData[]): Promise<void> => {
  await db.transaction().execute(async (tx) => {
    const insertedPosts = await tx
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
      .returning(['id', 'cid', 'did', 'rkey'])
      .execute();

    // Map composite key to id
    const compositeKeyToIdMap = new Map<string, bigint>();
    insertedPosts.forEach((post) => {
      const compositeKey = `${post.cid}-${post.did}-${post.rkey}`;
      compositeKeyToIdMap.set(compositeKey, BigInt(post.id));
    });

    // Prepare bulk insert for emojis
    const emojiInserts: { post_id: bigint; emoji: string; lang: string; created_at: Date }[] = [];
    batch.forEach((post) => {
      if (post.hasEmojis) {
        const compositeKey = `${post.cid}-${post.did}-${post.rkey}`;
        const postId = compositeKeyToIdMap.get(compositeKey);
        if (postId) {
          post.emojis.forEach((emoji) => {
            post.langs.forEach((lang) => {
              emojiInserts.push({
                post_id: postId,
                emoji: emoji,
                lang: lang,
                created_at: new Date(post.createdAt),
              });
            });
          });
        }
      }
    });

    if (emojiInserts.length > 0) {
      const emojiChunks = chunkArray(emojiInserts, EMOJI_BATCH_SIZE);
      for (const chunk of emojiChunks) {
        await tx.insertInto('post_emojis').values(chunk).execute();
      }
    }
  });
};

export const insertProfiles = async (batch: ProfileData[]): Promise<void> => {
  await db.transaction().execute(async (tx) => {
    const insertedProfiles = await tx
      .insertInto('profiles')
      .values(
        batch.map((profile) => ({
          cid: profile.cid,
          did: profile.did,
          rkey: profile.rkey,
          display_name: profile.displayName,
          description: profile.description,
          created_at: new Date(profile.createdAt),
          has_display_name_emojis: profile.hasDisplayNameEmojis,
          has_description_emojis: profile.hasDescriptionEmojis,
        })),
      )
      .returning(['id', 'cid', 'did', 'rkey'])
      .execute();

    // Map composite key to id
    const compositeKeyToIdMap = new Map<string, bigint>();
    insertedProfiles.forEach((profile) => {
      const compositeKey = `${profile.cid}-${profile.did}-${profile.rkey}`;
      compositeKeyToIdMap.set(compositeKey, BigInt(profile.id));
    });

    // Prepare bulk insert for emojis
    const displayNameEmojiInserts: { profile_id: bigint; emoji: string; created_at: Date }[] = [];
    const descriptionEmojiInserts: { profile_id: bigint; emoji: string; created_at: Date }[] = [];
    batch.forEach((profile) => {
      if (profile.hasDisplayNameEmojis) {
        const compositeKey = `${profile.cid}-${profile.did}-${profile.rkey}`;
        const profileId = compositeKeyToIdMap.get(compositeKey);
        if (profileId) {
          profile.displayNameEmojis.forEach((emoji) => {
            displayNameEmojiInserts.push({
              profile_id: profileId,
              emoji: emoji,
              created_at: new Date(profile.createdAt),
            });
          });
        }
      }

      if (profile.hasDescriptionEmojis) {
        const compositeKey = `${profile.cid}-${profile.did}-${profile.rkey}`;
        const profileId = compositeKeyToIdMap.get(compositeKey);
        if (profileId) {
          profile.descriptionEmojis.forEach((emoji) => {
            descriptionEmojiInserts.push({
              profile_id: profileId,
              emoji: emoji,
              created_at: new Date(profile.createdAt),
            });
          });
        }
      }
    });

    if (displayNameEmojiInserts.length > 0) {
      const displayNameEmojiChunks = chunkArray(displayNameEmojiInserts, EMOJI_BATCH_SIZE);
      for (const chunk of displayNameEmojiChunks) {
        await tx.insertInto('profile_display_name_emojis').values(chunk).execute();
      }
    }

    if (descriptionEmojiInserts.length > 0) {
      const descriptionEmojiChunks = chunkArray(descriptionEmojiInserts, EMOJI_BATCH_SIZE);
      for (const chunk of descriptionEmojiChunks) {
        await tx.insertInto('profile_description_emojis').values(chunk).execute();
      }
    }
  });
};
