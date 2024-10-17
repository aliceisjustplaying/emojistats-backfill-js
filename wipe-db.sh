#!/bin/bash
direnv allow
valkey-cli flushall
psql "$DATABASE_URL" -c '
TRUNCATE TABLE posts RESTART IDENTITY CASCADE;
TRUNCATE TABLE profiles RESTART IDENTITY CASCADE;
TRUNCATE TABLE post_emojis RESTART IDENTITY CASCADE;
TRUNCATE TABLE profile_display_name_emojis RESTART IDENTITY CASCADE;
TRUNCATE TABLE profile_description_emojis RESTART IDENTITY CASCADE;
'
