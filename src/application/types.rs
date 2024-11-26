use serde::{Deserialize, Serialize};
use surrealdb::{Datetime, RecordId};

#[derive(Debug, Serialize)]
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
}

#[derive(Debug, Serialize)]
pub struct BskyPost {
    pub author: RecordId,
    #[serde(rename = "bridgyOriginalUrl")]
    pub bridgy_original_url: Option<String>,
    #[serde(rename = "createdAt")]
    pub created_at: Option<Datetime>,
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
}

#[derive(Debug, Serialize)]
pub struct BskyPostImage {
    pub alt: String,
    pub blob: RecordId,
}

#[derive(Debug, Serialize)]
pub struct BskyPostVideo {
    pub alt: Option<String>,
    #[serde(rename = "aspectRatio")]
    pub aspect_ratio: BskyPostVideoAspectRatio,
}

#[derive(Debug, Serialize)]
pub struct BskyPostVideoAspectRatio {
    pub width: Option<u64>,
    pub height: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Record {
    id: RecordId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JetstreamCursor {
    pub time_us: u64,
}
