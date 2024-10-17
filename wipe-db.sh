#!/bin/bash
direnv allow
psql "$DATABASE_URL" -c '
TRUNCATE TABLE posts RESTART IDENTITY CASCADE;
TRUNCATE TABLE profiles RESTART IDENTITY CASCADE;
TRUNCATE TABLE emojis RESTART IDENTITY CASCADE;
'
