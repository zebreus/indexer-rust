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
    joined_via_starter_pack TEXT REFERENCES starterpack(id) DEFERRABLE,
    -- Constraint is applied in the down migration script, because the post table is created after did table
    pinned_post TEXT,
    created_at TIMESTAMP WITH TIME ZONE,
    seen_at TIMESTAMP WITH TIME ZONE NOT NULL,
    extra_data TEXT
);

CREATE TABLE IF NOT EXISTS did_label (
    did_id TEXT NOT NULL REFERENCES did(id) DEFERRABLE,
    label TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS post (
    id TEXT PRIMARY KEY,
    author TEXT NOT NULL, -- REFERENCES did(id),
    bridgy_original_url TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    parent TEXT, -- REFERENCES post(id) DEFERRABLE,
    record TEXT,
    root TEXT, -- REFERENCES post(id) DEFERRABLE,
    text TEXT NOT NULL,
    via TEXT,
    video JSONB,
    extra_data TEXT
);
ALTER TABLE did ADD CONSTRAINT fk_pinned_post FOREIGN KEY (pinned_post) REFERENCES post(id) DEFERRABLE;

CREATE TABLE IF NOT EXISTS post_label (
    post_id TEXT NOT NULL REFERENCES post(id) DEFERRABLE,
    label TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS post_lang (
    post_id TEXT NOT NULL REFERENCES post(id) DEFERRABLE,
    lang TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS post_link (
    post_id TEXT NOT NULL REFERENCES post(id) DEFERRABLE,
    link TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS post_tag (
    post_id TEXT NOT NULL REFERENCES post(id) DEFERRABLE,
    tag TEXT NOT NULL
);


CREATE TABLE IF NOT EXISTS post_image (
    id SERIAL PRIMARY KEY,
    post_id TEXT NOT NULL REFERENCES post(id) DEFERRABLE,
    alt TEXT NOT NULL,
    blob_id TEXT NOT NULL, -- REFERENCES blob(id),
    aspect_ratio_width INT,
    aspect_ratio_height INT
);

CREATE TABLE IF NOT EXISTS post_mention (
    id TEXT PRIMARY KEY,
    post_id TEXT NOT NULL REFERENCES post(id),
    mentioned_did_id TEXT NOT NULL REFERENCES did(id) DEFERRABLE
);

CREATE TABLE IF NOT EXISTS feed (
    id TEXT PRIMARY KEY,
    uri TEXT NOT NULL,
    author TEXT NOT NULL REFERENCES did(id) DEFERRABLE,
    rkey TEXT NOT NULL,
    did TEXT NOT NULL,
    display_name TEXT NOT NULL,
    description TEXT,
    avatar TEXT REFERENCES blob(id) DEFERRABLE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    extra_data TEXT
);

CREATE TABLE IF NOT EXISTS list (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    purpose TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    description TEXT,
    avatar TEXT REFERENCES blob(id) DEFERRABLE,
    labels TEXT[],
    extra_data TEXT
);

-- Relation tables
CREATE TABLE IF NOT EXISTS block (
    id TEXT PRIMARY KEY,
    blocker_did_id TEXT NOT NULL REFERENCES did(id) DEFERRABLE,
    blocked_did_id TEXT NOT NULL REFERENCES did(id) DEFERRABLE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS follow (
    id TEXT PRIMARY KEY,
    follower_did_id TEXT NOT NULL, -- REFERENCES did(id) DEFERRABLE,
    followed_did_id TEXT NOT NULL, -- REFERENCES did(id) DEFERRABLE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS "like" (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES did(id) DEFERRABLE,
    target_post_id TEXT REFERENCES post(id) DEFERRABLE,
    target_feed_id TEXT REFERENCES feed(id) DEFERRABLE,
    target_list_id TEXT REFERENCES list(id) DEFERRABLE,
    target_starterpack_id TEXT REFERENCES starterpack(id) DEFERRABLE,
    target_labeler_id TEXT REFERENCES labeler(id) DEFERRABLE,
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
    list_id TEXT NOT NULL REFERENCES list(id) DEFERRABLE,
    did_id TEXT NOT NULL REFERENCES did(id) DEFERRABLE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS posts_relation (
    id TEXT PRIMARY KEY,
    did_id TEXT NOT NULL REFERENCES did(id) DEFERRABLE,
    post_id TEXT NOT NULL REFERENCES post(id) DEFERRABLE
);

CREATE TABLE IF NOT EXISTS replies_relation (
    id TEXT PRIMARY KEY,
    did_id TEXT NOT NULL REFERENCES did(id) DEFERRABLE,
    post_id TEXT NOT NULL REFERENCES post(id) DEFERRABLE
);

CREATE TABLE IF NOT EXISTS quotes_relation (
    id TEXT PRIMARY KEY,
    source_post_id TEXT NOT NULL REFERENCES post(id) DEFERRABLE,
    target_post_id TEXT NOT NULL REFERENCES post(id) DEFERRABLE
);

CREATE TABLE IF NOT EXISTS replyto_relation (
    id TEXT PRIMARY KEY,
    source_post_id TEXT NOT NULL REFERENCES post(id) DEFERRABLE,
    target_post_id TEXT NOT NULL REFERENCES post(id) DEFERRABLE
);

CREATE TABLE IF NOT EXISTS repost (
    id TEXT PRIMARY KEY,
    did_id TEXT NOT NULL REFERENCES did(id) DEFERRABLE,
    post_id TEXT NOT NULL REFERENCES post(id) DEFERRABLE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS latest_backfill (
    id TEXT PRIMARY KEY,
    of_did_id TEXT NOT NULL,
    at TIMESTAMP WITH TIME ZONE NULL
);

COMMIT;