use anyhow::Context;
use serde::{Deserialize, Serialize};
use surrealdb::{engine::any::Any, Datetime, RecordId, Surreal};
use tracing::{debug, info};

/// Database struct for a bluesky profile
#[derive(Debug, Serialize)]
#[allow(dead_code)]
pub struct BskyProfile {
    #[serde(rename = "displayName")]
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub avatar: Option<RecordId>,
    pub banner: Option<RecordId>,
    #[serde(rename = "createdAt")]
    pub created_at: Option<Datetime>,
    #[serde(rename = "seenAt")]
    pub seen_at: Datetime,
    #[serde(rename = "joinedViaStarterPack")]
    pub joined_via_starter_pack: Option<RecordId>,
    pub labels: Option<Vec<String>>,
    #[serde(rename = "pinnedPost")]
    pub pinned_post: Option<RecordId>,
    #[serde(rename = "extraData")]
    pub extra_data: Option<String>,
}

/// Database struct for a record
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct Record {
    id: RecordId,
}

/// Database struct for a jetstream cursor
#[derive(Debug, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct JetstreamCursor {
    pub time_us: u64,
}

/// Database struct for a jetstream account event
#[derive(Debug, Serialize, Deserialize)]
pub struct JetstreamAccountEvent {
    pub time_us: u64,
    pub active: bool,
    pub seq: u64,
    pub time: String,
}

/// Database struct for a jetstream identity event
#[derive(Debug, Serialize, Deserialize)]
pub struct JetstreamIdentityEvent {
    pub time_us: u64,
    pub handle: String,
    pub seq: u64,
    pub time: String,
}

/// Database struct for a bluesky post
#[derive(Debug, Serialize)]
pub struct BskyPost {
    pub author: RecordId,
    #[serde(rename = "bridgyOriginalUrl")]
    pub bridgy_original_url: Option<String>,
    #[serde(rename = "createdAt")]
    pub created_at: Datetime,
    pub images: Option<Vec<BskyPostImage>>,
    pub labels: Option<Vec<String>>,
    pub langs: Option<Vec<String>>,
    pub links: Option<Vec<String>>,
    pub mentions: Option<Vec<RecordId>>,
    pub parent: Option<RecordId>,
    pub record: Option<RecordId>,
    pub root: Option<RecordId>,
    pub tags: Option<Vec<String>>,
    pub text: String,
    pub via: Option<String>,
    pub video: Option<BskyPostVideo>,
    #[serde(rename = "extraData")]
    pub extra_data: Option<String>,
}

/// Database struct for a bluesky post image
#[derive(Debug, Serialize)]
pub struct BskyPostImage {
    pub alt: String,
    pub blob: RecordId,
    #[serde(rename = "aspectRatio")]
    pub aspect_ratio: Option<BskyPostMediaAspectRatio>,
}

/// Database struct for a bluesky post video
#[derive(Debug, Serialize)]
pub struct BskyPostVideo {
    pub alt: Option<String>,
    #[serde(rename = "aspectRatio")]
    pub aspect_ratio: Option<BskyPostMediaAspectRatio>,
    pub blob: BskyPostVideoBlob,
    pub captions: Option<Vec<BskyPostVideoCaption>>,
}

#[derive(Debug, Serialize)]
pub struct BskyPostVideoCaption {}

#[derive(Debug, Serialize)]
pub struct BskyPostVideoBlob {
    pub cid: String,
    #[serde(rename = "mediaType")]
    pub media_type: String,
    pub size: u64,
}

/// Database struct for a bluesky post video aspect ratio
#[derive(Debug, Serialize)]
pub struct BskyPostMediaAspectRatio {
    pub width: u64,
    pub height: u64,
}

#[derive(Debug, Serialize)]
pub struct BskyFeed {
    pub uri: String,
    pub author: RecordId,
    pub rkey: String,
    pub did: String,
    #[serde(rename = "displayName")]
    pub display_name: String,
    pub description: Option<String>,
    pub avatar: Option<RecordId>,
    #[serde(rename = "createdAt")]
    pub created_at: Datetime,
    #[serde(rename = "extraData")]
    pub extra_data: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct BskyList {
    pub name: String,
    pub purpose: String,
    #[serde(rename = "createdAt")]
    pub created_at: Datetime,
    pub description: Option<String>,
    pub avatar: Option<RecordId>,
    pub labels: Option<Vec<String>>,
    #[serde(rename = "extraData")]
    pub extra_data: Option<String>,
}

/// Initialize the database with the necessary definitions
pub async fn init(db: &Surreal<Any>) -> anyhow::Result<()> {
    // define the namespace
    debug!(target: "indexer", "Defining namespace");
    db.query("DEFINE NAMESPACE atp;")
        .await
        .context("Failed to define namespace atp")?;
    db.use_ns("atp").await?;

    // define the database
    debug!(target: "indexer", "Defining database");
    db.query("DEFINE DATABASE atp;")
        .await
        .context("Failed to define database atp")?;
    db.use_ns("atp").use_db("atp").await?;

    // TODO Add all types
    db.query(
        "
DEFINE TABLE did SCHEMAFULL;
DEFINE FIELD handle ON TABLE did TYPE option<string>;
DEFINE FIELD displayName ON TABLE did TYPE option<string>;
DEFINE FIELD description ON TABLE did TYPE option<string>;
DEFINE FIELD avatar ON TABLE did TYPE option<record<blob>>;
DEFINE FIELD banner ON TABLE did TYPE option<record<blob>>;
DEFINE FIELD labels ON TABLE did TYPE option<array<string>>;
DEFINE FIELD joinedViaStarterPack ON TABLE did TYPE option<record<starterpack>>;
DEFINE FIELD pinnedPost ON TABLE did TYPE option<record<post>>;
DEFINE FIELD createdAt ON TABLE did TYPE option<datetime>;
DEFINE FIELD seenAt ON TABLE did TYPE datetime;
DEFINE FIELD extraData ON TABLE did TYPE option<string>;

DEFINE TABLE post SCHEMAFULL;
DEFINE FIELD author ON TABLE post TYPE record<did>;
DEFINE FIELD bridgyOriginalUrl ON TABLE post TYPE option<string>;
DEFINE FIELD createdAt ON TABLE post TYPE datetime;
DEFINE FIELD images ON TABLE post TYPE option<array>;
DEFINE FIELD images.* ON TABLE post TYPE object;
DEFINE FIELD images.*.alt ON TABLE post TYPE string;
DEFINE FIELD images.*.blob ON TABLE post TYPE record<blob>;
DEFINE FIELD images.*.aspectRatio ON TABLE post TYPE option<object>;
DEFINE FIELD images.*.aspectRatio.height ON TABLE post TYPE option<int>;
DEFINE FIELD images.*.aspectRatio.width ON TABLE post TYPE option<int>;
DEFINE FIELD labels ON TABLE post TYPE option<array<string>>;
DEFINE FIELD langs ON TABLE post TYPE option<array<string>>;
DEFINE FIELD links ON TABLE post TYPE option<array<string>>;
DEFINE FIELD mentions ON TABLE post TYPE option<array<record<did>>>;
DEFINE FIELD parent ON TABLE post TYPE option<record<post>>;
DEFINE FIELD record ON TABLE post TYPE option<record>;
DEFINE FIELD root ON TABLE post TYPE option<record<post>>;
DEFINE FIELD tags ON TABLE post TYPE option<array<string>>;
DEFINE FIELD text ON TABLE post TYPE string;
DEFINE FIELD via ON TABLE post TYPE option<string>;
DEFINE FIELD video ON TABLE post TYPE option<object>;
DEFINE FIELD video.alt ON TABLE post TYPE option<string>;
DEFINE FIELD video.aspectRatio ON TABLE post TYPE option<object>;
DEFINE FIELD video.aspectRatio.height ON TABLE post TYPE option<int>;
DEFINE FIELD video.aspectRatio.width ON TABLE post TYPE option<int>;
DEFINE FIELD video.blob ON TABLE post TYPE option<object>;
DEFINE FIELD video.blob.cid ON TABLE post TYPE option<string>;
DEFINE FIELD video.blob.mediaType ON TABLE post TYPE option<string>;
DEFINE FIELD video.blob.size ON TABLE post TYPE option<int>;
DEFINE FIELD video.captions ON TABLE post TYPE option<array<object>>;
DEFINE FIELD extraData ON TABLE post TYPE option<string>;

DEFINE TABLE feed SCHEMAFULL;
DEFINE FIELD uri ON TABLE feed TYPE string;
DEFINE FIELD author ON TABLE feed TYPE record<did>;
DEFINE FIELD rkey ON TABLE feed TYPE string;
DEFINE FIELD did ON TABLE feed TYPE string;
DEFINE FIELD displayName ON TABLE feed TYPE string;
DEFINE FIELD description ON TABLE feed TYPE option<string>;
DEFINE FIELD avatar ON TABLE feed TYPE option<record<blob>>;
DEFINE FIELD createdAt ON TABLE feed TYPE datetime;
DEFINE FIELD extraData ON TABLE feed TYPE option<string>;

DEFINE TABLE list SCHEMAFULL;
DEFINE FIELD name ON TABLE list TYPE string;
DEFINE FIELD purpose ON TABLE list TYPE string;
DEFINE FIELD createdAt ON TABLE list TYPE datetime;
DEFINE FIELD description ON TABLE list TYPE option<string>;
DEFINE FIELD avatar ON TABLE list TYPE option<record<blob>>;
DEFINE FIELD labels ON TABLE list TYPE option<array<string>>;
DEFINE FIELD extraData ON TABLE list TYPE option<string>;

DEFINE TABLE block SCHEMAFULL TYPE RELATION FROM did TO did;
DEFINE FIELD createdAt ON TABLE block TYPE datetime;

DEFINE TABLE follow SCHEMAFULL TYPE RELATION FROM did TO did;
DEFINE FIELD createdAt ON TABLE follow TYPE datetime;

DEFINE TABLE like SCHEMAFULL TYPE RELATION FROM did TO post|feed|list|starterpack|labeler;
DEFINE FIELD createdAt ON TABLE like TYPE datetime;

DEFINE TABLE listitem SCHEMAFULL TYPE RELATION FROM list TO did;
DEFINE FIELD createdAt ON TABLE listitem TYPE datetime;

DEFINE TABLE posts SCHEMAFULL TYPE RELATION FROM did TO post;
DEFINE TABLE replies SCHEMAFULL TYPE RELATION FROM did TO post;

DEFINE TABLE quotes SCHEMAFULL TYPE RELATION FROM post TO post;

DEFINE TABLE replyto SCHEMAFULL TYPE RELATION FROM post TO post;

DEFINE TABLE repost SCHEMAFULL TYPE RELATION FROM did TO post;
DEFINE FIELD createdAt ON TABLE repost TYPE datetime;

// DEFINE TABLE like_count_view TYPE NORMAL AS
// SELECT
//   count() AS c,
//   ->out.id AS out
//   FROM like
//   GROUP BY out
// ;

// DEFINE TABLE repost_count_view TYPE NORMAL AS
// SELECT
//   count() AS c,
//   ->out.id AS out
//   FROM repost
//   GROUP BY out
// ;

// DEFINE TABLE reply_count_view TYPE NORMAL AS
// SELECT
//   count() AS c,
//   ->out.id AS out
//   FROM replyto
//   GROUP BY out
// ;

// DEFINE TABLE quote_count_view TYPE NORMAL AS
// SELECT
//   count() AS c,
//   ->out.id AS out
//   FROM quotes
//   GROUP BY out
// ;

// DEFINE TABLE following_count_view TYPE NORMAL AS
// SELECT
//   count() AS c,
//   ->in.id AS in
//   FROM follow
//   GROUP BY in
// ;

// DEFINE TABLE follower_count_view TYPE NORMAL AS
// SELECT
//   count() AS c,
//   ->out.id AS out
//   FROM follow
//   GROUP BY out
// ;

DEFINE TABLE latest_backfill SCHEMAFULL;
DEFINE FIELD of ON TABLE latest_backfill TYPE record<did>;
DEFINE FIELD at ON TABLE latest_backfill TYPE option<datetime>;
        ", // record<one | two>
    )
    .await?;

    Ok(())
}
