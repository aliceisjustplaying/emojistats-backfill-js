import 'dotenv/config';
import pg from 'pg';

const { Client } = pg;

const client = new Client({
  connectionString: process.env.DATABASE_URL!,
});

await client.connect();

export async function createTables() {
  await client.query(`
    CREATE TABLE IF NOT EXISTS posts (
      id BIGSERIAL PRIMARY KEY,
      cid TEXT NOT NULL, -- ~64 characters
      did TEXT NOT NULL, -- ~32 characters
      rkey TEXT NOT NULL, -- ~13 characters
      has_emojis BOOLEAN NOT NULL DEFAULT FALSE,
      langs TEXT[] NOT NULL DEFAULT '{}',
      text TEXT,
      created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT (now() at time zone 'utc'),
      UNIQUE (did, cid, rkey)
    );

    CREATE TABLE IF NOT EXISTS emojis (
      id BIGSERIAL PRIMARY KEY,
      post_id BIGINT DEFAULT NULL,
      profile_id BIGINT DEFAULT NULL,
      emoji TEXT NOT NULL,
      lang TEXT NOT NULL, -- 2 or 5 characters
      created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT (now() at time zone 'utc')
    );

    CREATE TABLE IF NOT EXISTS profiles (
      id BIGSERIAL PRIMARY KEY,
      cid TEXT NOT NULL, -- ~64 characters
      did TEXT NOT NULL, -- ~32 characters
      rkey TEXT NOT NULL, -- ~13 characters
      description TEXT,
      display_name TEXT,
      created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT (now() at time zone 'utc'),
      UNIQUE (did, cid, rkey)
    );
  `);
}

createTables()
  .catch((e: unknown) => {
    console.error(e);
  })
  .finally(() => void client.end());
