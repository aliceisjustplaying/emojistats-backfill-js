import { BATCH_SIZE, BATCH_TIMEOUT_MS } from '../constants.js';
import { PostData, ProfileData } from '../types.js';
import { insertPosts, insertProfiles } from './insertFunctions.js';
import { PostgresBatchQueue } from './postgresBatchQueue.js';

export const postBatchQueue = new PostgresBatchQueue<PostData>(BATCH_SIZE, BATCH_TIMEOUT_MS, insertPosts);
export const profileBatchQueue = new PostgresBatchQueue<ProfileData>(BATCH_SIZE, BATCH_TIMEOUT_MS, insertProfiles);
