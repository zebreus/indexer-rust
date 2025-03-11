use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use surrealdb::RecordId;

/// Database struct for a bluesky profile
#[derive(Debug, Clone, Serialize)]
#[allow(dead_code)]
pub struct BskyDid {
    #[serde(alias = "displayName")]
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub avatar: Option<RecordId>,
    pub banner: Option<RecordId>,
    #[serde(alias = "createdAt")]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(alias = "seenAt")]
    pub seen_at: DateTime<Utc>,
    #[serde(alias = "joinedViaStarterPack")]
    pub joined_via_starter_pack: Option<RecordId>,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(alias = "pinnedPost")]
    pub pinned_post: Option<RecordId>,
    #[serde(alias = "extraData")]
    pub extra_data: Option<String>,
}

/// Database struct for a jetstream cursor
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct JetstreamCursor {
    pub time_us: u64,
}

/// Database struct for a jetstream account event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JetstreamAccountEvent {
    pub time_us: i64,
    pub active: bool,
    pub seq: i64,
    pub time: String,
}

/// Database struct for a jetstream identity event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JetstreamIdentityEvent {
    pub time_us: i64,
    pub handle: String,
    pub seq: i64,
    pub time: String,
}

/// Database struct for a bluesky post
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BskyPost {
    pub author: RecordId,
    #[serde(rename = "bridgyOriginalUrl")]
    pub bridgy_original_url: Option<String>,
    #[serde(rename = "createdAt")]
    pub created_at: DateTime<Utc>,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BskyPostImage {
    pub alt: String,
    pub blob: RecordId,
    #[serde(rename = "aspectRatio")]
    pub aspect_ratio: Option<BskyPostMediaAspectRatio>,
}

/// Database struct for a bluesky post video
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BskyPostVideo {
    pub alt: Option<String>,
    #[serde(rename = "aspectRatio")]
    pub aspect_ratio: Option<BskyPostMediaAspectRatio>,
    pub blob: BskyPostVideoBlob,
    pub captions: Option<Vec<BskyPostVideoCaption>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BskyPostVideoCaption {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BskyPostVideoBlob {
    pub cid: String,
    #[serde(rename = "mediaType")]
    pub media_type: String,
    pub size: u64,
}

/// Database struct for a bluesky post video aspect ratio
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BskyPostMediaAspectRatio {
    pub width: u64,
    pub height: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub created_at: DateTime<Utc>,
    #[serde(rename = "extraData")]
    pub extra_data: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BskyList {
    pub name: String,
    pub purpose: String,
    #[serde(rename = "createdAt")]
    pub created_at: DateTime<Utc>,
    pub description: Option<String>,
    pub avatar: Option<RecordId>,
    pub labels: Option<Vec<String>>,
    #[serde(rename = "extraData")]
    pub extra_data: Option<String>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BskyLatestBackfill {
    pub of: RecordId,
    pub at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BskyFollow {
    #[serde(rename = "in")]
    pub from: RecordId,
    #[serde(rename = "out")]
    pub to: RecordId,
    #[serde(rename = "createdAt")]
    pub created_at: DateTime<Utc>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BskyLike {
    #[serde(rename = "in")]
    pub from: RecordId,
    #[serde(rename = "out")]
    pub to: RecordId,
    #[serde(rename = "createdAt")]
    pub created_at: DateTime<Utc>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BskyRepost {
    #[serde(rename = "in")]
    pub from: RecordId,
    #[serde(rename = "out")]
    pub to: RecordId,
    #[serde(rename = "createdAt")]
    pub created_at: DateTime<Utc>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BskyBlock {
    #[serde(rename = "in")]
    pub from: RecordId,
    #[serde(rename = "out")]
    pub to: RecordId,
    #[serde(rename = "createdAt")]
    pub created_at: DateTime<Utc>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BskyListBlock {
    #[serde(rename = "in")]
    pub from: RecordId,
    #[serde(rename = "out")]
    pub to: RecordId,
    #[serde(rename = "createdAt")]
    pub created_at: DateTime<Utc>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BskyListItem {
    #[serde(rename = "in")]
    pub from: RecordId,
    #[serde(rename = "out")]
    pub to: RecordId,
    #[serde(rename = "createdAt")]
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BskyQuote {
    #[serde(rename = "in")]
    pub from: RecordId,
    #[serde(rename = "out")]
    pub to: RecordId,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BskyRepliesRelation {
    #[serde(rename = "in")]
    pub from: RecordId,
    #[serde(rename = "out")]
    pub to: RecordId,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BskyReplyToRelation {
    #[serde(rename = "in")]
    pub from: RecordId,
    #[serde(rename = "out")]
    pub to: RecordId,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BskyPostsRelation {
    #[serde(rename = "in")]
    pub from: RecordId,
    #[serde(rename = "out")]
    pub to: RecordId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithId<R: Serialize> {
    pub id: String,
    #[serde(flatten)]
    pub data: R,
}
