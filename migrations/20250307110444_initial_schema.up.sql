-- Add up migration script here
-- Create tables with text primary keys and preserved names converted to snake_case
-- Generated with AI from the surrealdb schema

BEGIN;

CREATE TABLE IF NOT EXISTS blob (
    id TEXT PRIMARY KEY,
    cid TEXT NOT NULL,
    media_type TEXT NOT NULL,
    size BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS starterpack (
    id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS labeler (
    id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS did (
    id TEXT PRIMARY KEY,
    handle TEXT,
    display_name TEXT,
    description TEXT,
    avatar TEXT REFERENCES blob(id),
    banner TEXT REFERENCES blob(id),
    joined_via_starter_pack TEXT REFERENCES starterpack(id),
    -- Constraint is applied in the down migration script, because the post table is created after did table
    pinned_post TEXT,
    created_at TIMESTAMP WITH TIME ZONE,
    seen_at TIMESTAMP WITH TIME ZONE NOT NULL,
    labels TEXT[] NOT NULL,
    extra_data TEXT
);

CREATE TABLE IF NOT EXISTS post (
    id TEXT PRIMARY KEY,
    author TEXT NOT NULL REFERENCES did(id),
    bridgy_original_url TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    labels TEXT[],
    langs TEXT[],
    links TEXT[],
    parent TEXT REFERENCES post(id),
    record TEXT,
    root TEXT REFERENCES post(id),
    tags TEXT[],
    text TEXT NOT NULL,
    via TEXT,
    video JSONB,
    extra_data TEXT
);

ALTER TABLE did ADD CONSTRAINT fk_pinned_post FOREIGN KEY (pinned_post) REFERENCES post(id) DEFERRABLE;

CREATE TABLE IF NOT EXISTS post_image (
    id TEXT PRIMARY KEY,
    post_id TEXT NOT NULL REFERENCES post(id),
    alt TEXT NOT NULL,
    blob_id TEXT NOT NULL REFERENCES blob(id),
    aspect_ratio_width BIGINT,
    aspect_ratio_height BIGINT
);

CREATE TABLE IF NOT EXISTS post_mention (
    id TEXT PRIMARY KEY,
    post_id TEXT NOT NULL REFERENCES post(id),
    mentioned_did_id TEXT NOT NULL REFERENCES did(id)
);

CREATE TABLE IF NOT EXISTS feed (
    id TEXT PRIMARY KEY,
    uri TEXT NOT NULL,
    author TEXT NOT NULL REFERENCES did(id),
    rkey TEXT NOT NULL,
    did TEXT NOT NULL,
    display_name TEXT NOT NULL,
    description TEXT,
    avatar TEXT REFERENCES blob(id),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    extra_data TEXT
);

CREATE TABLE IF NOT EXISTS list (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    purpose TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    description TEXT,
    avatar TEXT REFERENCES blob(id),
    labels TEXT[],
    extra_data TEXT
);

-- Relation tables
CREATE TABLE IF NOT EXISTS block (
    id TEXT PRIMARY KEY,
    blocker_did_id TEXT NOT NULL REFERENCES did(id),
    blocked_did_id TEXT NOT NULL REFERENCES did(id),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS follow (
    id TEXT PRIMARY KEY,
    follower_did_id TEXT NOT NULL REFERENCES did(id),
    followed_did_id TEXT NOT NULL REFERENCES did(id),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS "like" (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES did(id),
    target_post_id TEXT REFERENCES post(id),
    target_feed_id TEXT REFERENCES feed(id),
    target_list_id TEXT REFERENCES list(id),
    target_starterpack_id TEXT REFERENCES starterpack(id),
    target_labeler_id TEXT REFERENCES labeler(id),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    CHECK (
        (target_post_id IS NOT NULL)::integer +
        (target_feed_id IS NOT NULL)::integer +
        (target_list_id IS NOT NULL)::integer +
        (target_starterpack_id IS NOT NULL)::integer +
        (target_labeler_id IS NOT NULL)::integer = 1
    )
);

CREATE TABLE IF NOT EXISTS listitem (
    id TEXT PRIMARY KEY,
    list_id TEXT NOT NULL REFERENCES list(id),
    did_id TEXT NOT NULL REFERENCES did(id),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS posts_relation (
    id TEXT PRIMARY KEY,
    did_id TEXT NOT NULL REFERENCES did(id),
    post_id TEXT NOT NULL REFERENCES post(id)
);

CREATE TABLE IF NOT EXISTS replies_relation (
    id TEXT PRIMARY KEY,
    did_id TEXT NOT NULL REFERENCES did(id),
    post_id TEXT NOT NULL REFERENCES post(id)
);

CREATE TABLE IF NOT EXISTS quotes_relation (
    id TEXT PRIMARY KEY,
    source_post_id TEXT NOT NULL REFERENCES post(id),
    target_post_id TEXT NOT NULL REFERENCES post(id)
);

CREATE TABLE IF NOT EXISTS replyto_relation (
    id TEXT PRIMARY KEY,
    source_post_id TEXT NOT NULL REFERENCES post(id),
    target_post_id TEXT NOT NULL REFERENCES post(id)
);

CREATE TABLE IF NOT EXISTS repost (
    id TEXT PRIMARY KEY,
    did_id TEXT NOT NULL REFERENCES did(id),
    post_id TEXT NOT NULL REFERENCES post(id),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS latest_backfill (
    id TEXT PRIMARY KEY,
    of_did_id TEXT NOT NULL,
    at TIMESTAMP WITH TIME ZONE NULL
);

COMMIT;