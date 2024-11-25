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

#[derive(Debug, Deserialize)]
pub struct Record {
    id: RecordId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JetstreamCursor {
    pub time_us: u64,
}
