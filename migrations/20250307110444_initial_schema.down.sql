-- Add down migration script here
BEGIN;
DROP TABLE IF EXISTS blob CASCADE;
DROP TABLE IF EXISTS starterpack CASCADE;
DROP TABLE IF EXISTS labeler CASCADE;
DROP TABLE IF EXISTS did CASCADE;
DROP TABLE IF EXISTS did_label CASCADE;
DROP TABLE IF EXISTS post CASCADE;
DROP TABLE IF EXISTS post_image CASCADE;
DROP TABLE IF EXISTS post_mention CASCADE;
DROP TABLE IF EXISTS post_label CASCADE;
DROP TABLE IF EXISTS post_lang CASCADE;
DROP TABLE IF EXISTS post_link CASCADE;
DROP TABLE IF EXISTS post_tag CASCADE;
DROP TABLE IF EXISTS post_relation CASCADE;
DROP TABLE IF EXISTS feed CASCADE;
DROP TABLE IF EXISTS list CASCADE;
DROP TABLE IF EXISTS "block" CASCADE;
DROP TABLE IF EXISTS follow CASCADE;
DROP TABLE IF EXISTS "like" CASCADE;
DROP TABLE IF EXISTS listitem CASCADE;
DROP TABLE IF EXISTS posts_relation CASCADE;
DROP TABLE IF EXISTS replies_relation CASCADE;
DROP TABLE IF EXISTS quotes_relation CASCADE;
DROP TABLE IF EXISTS replyto_relation CASCADE;
DROP TABLE IF EXISTS repost CASCADE;
DROP TABLE IF EXISTS latest_backfill CASCADE;
COMMIT;