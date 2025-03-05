use super::{
    definitions::{
        BskyPost, BskyPostImage, BskyPostMediaAspectRatio, BskyPostVideo, BskyPostVideoBlob,
    },
    utils::{self, at_uri_to_record_id, blob_ref_to_record_id, did_to_key},
};
use anyhow::Result;
use atrium_api::app::bsky::richtext::facet::MainFeaturesItem;
use atrium_api::types::Object;
use atrium_api::{
    app::bsky::embed::video,
    record::KnownRecord,
    types::{
        string::{Did, RecordKey},
        Blob, BlobRef,
    },
};
use chrono::Utc;
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{global, KeyValue};
use serde::Serialize;
use serde_with::skip_serializing_none;
use std::future::IntoFuture;
use std::sync::LazyLock;
use std::time::Instant;
use surrealdb::Datetime;
use surrealdb::{engine::any::Any, RecordId, Surreal};
use tracing::{debug, instrument, span, trace, warn, Instrument, Level};

#[derive(Serialize)]
struct UpdateFollow {
    #[serde(rename = "in")]
    pub from: surrealdb::RecordId,
    #[serde(rename = "out")]
    pub to: surrealdb::RecordId,
    pub id: String,
    #[serde(rename = "createdAt")]
    pub created_at: surrealdb::Datetime,
}

#[derive(Serialize)]
struct UpdateLike {
    #[serde(rename = "in")]
    pub from: surrealdb::RecordId,
    #[serde(rename = "out")]
    pub to: surrealdb::RecordId,
    pub id: String,
    #[serde(rename = "createdAt")]
    pub created_at: surrealdb::Datetime,
}

#[derive(Serialize)]
struct UpdateRepost {
    #[serde(rename = "in")]
    pub from: surrealdb::RecordId,
    #[serde(rename = "out")]
    pub to: surrealdb::RecordId,
    pub id: String,
    #[serde(rename = "createdAt")]
    pub created_at: surrealdb::Datetime,
}

#[derive(Serialize)]
struct UpdateBlock {
    #[serde(rename = "in")]
    pub from: surrealdb::RecordId,
    #[serde(rename = "out")]
    pub to: surrealdb::RecordId,
    pub id: String,
    #[serde(rename = "createdAt")]
    pub created_at: surrealdb::Datetime,
}

#[derive(Serialize)]
struct UpdateListBlock {
    #[serde(rename = "in")]
    pub from: surrealdb::RecordId,
    #[serde(rename = "out")]
    pub to: surrealdb::RecordId,
    pub id: String,
    #[serde(rename = "createdAt")]
    pub created_at: surrealdb::Datetime,
}

#[derive(Serialize)]
struct UpdateListItem {
    #[serde(rename = "in")]
    pub from: surrealdb::RecordId,
    #[serde(rename = "out")]
    pub to: surrealdb::RecordId,
    pub id: String,
    #[serde(rename = "createdAt")]
    pub created_at: surrealdb::Datetime,
}

#[skip_serializing_none]
#[derive(Serialize)]
struct UpdateLatestBackfill {
    of: surrealdb::RecordId,
    id: String,
    at: Option<surrealdb::sql::Datetime>,
}

/// Database struct for a bluesky profile
#[derive(Debug, Serialize)]
#[allow(dead_code)]
pub struct UpdateDid {
    pub id: String,
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

#[derive(Serialize)]
pub struct UpdateFeed {
    pub id: String,
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
pub struct UpdateList {
    pub id: String,
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

#[derive(Serialize)]
struct UpdateQuote {
    #[serde(rename = "in")]
    pub from: surrealdb::RecordId,
    #[serde(rename = "out")]
    pub to: surrealdb::RecordId,
    pub id: String,
}

#[derive(Serialize)]
struct UpdateRepliesRelation {
    #[serde(rename = "in")]
    pub from: surrealdb::RecordId,
    #[serde(rename = "out")]
    pub to: surrealdb::RecordId,
    pub id: String,
}

#[derive(Serialize)]
struct UpdateReplyToRelation {
    #[serde(rename = "in")]
    pub from: surrealdb::RecordId,
    #[serde(rename = "out")]
    pub to: surrealdb::RecordId,
    pub id: String,
}

#[derive(Serialize)]
struct UpdatePostsRelation {
    #[serde(rename = "in")]
    pub from: surrealdb::RecordId,
    #[serde(rename = "out")]
    pub to: surrealdb::RecordId,
    pub id: String,
}

#[derive(Serialize)]
struct WithId<R: Serialize> {
    id: String,
    #[serde(flatten)]
    data: R,
}

static QUERY_DURATION_METRIC: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    global::meter("indexer")
        .u64_histogram("indexer.database.insert_duration")
        .with_unit("ms")
        .with_description("Big update duration")
        .with_boundaries(vec![
            0.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 2500.0, 5000.0,
            7500.0, 10000.0, 25000.0, 50000.0, 75000.0, 100000.0, 250000.0, 500000.0, 750000.0,
            1000000.0, 2500000.0,
        ])
        .build()
});
static INSERTED_ROWS_METRIC: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("indexer")
        .u64_counter("indexer.database.inserted_elements")
        .with_unit("rows")
        .with_description("Inserted or updated rows")
        .build()
});
static INSERTED_SIZE_METRIC: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("indexer")
        .u64_counter("indexer.database.inserted_bytes")
        .with_unit("By")
        .with_description("Inserted or updated bytes (approximation)")
        .build()
});
static TRANSACTIONS_METRIC: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("indexer")
        .u64_counter("indexer.database.transactions")
        .with_unit("By")
        .with_description("Number of transactions")
        .build()
});

struct BigUpdateInfoRow {
    count: u64,
    size: u64,
}
impl core::fmt::Debug for BigUpdateInfoRow {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_map()
            .entry(&"count", &self.count)
            .entry(&"mB", &(self.size as f64 / 1024.0 / 1024.0))
            .finish()
    }
}

struct BigUpdateInfo {
    // Info about individual tables
    did: BigUpdateInfoRow,
    follows: BigUpdateInfoRow,
    latest_backfills: BigUpdateInfoRow,
    likes: BigUpdateInfoRow,
    reposts: BigUpdateInfoRow,
    blocks: BigUpdateInfoRow,
    listblocks: BigUpdateInfoRow,
    listitems: BigUpdateInfoRow,
    feeds: BigUpdateInfoRow,
    lists: BigUpdateInfoRow,
    threadgates: BigUpdateInfoRow,
    starterpacks: BigUpdateInfoRow,
    postgates: BigUpdateInfoRow,
    actordeclarations: BigUpdateInfoRow,
    labelerservices: BigUpdateInfoRow,
    quotes: BigUpdateInfoRow,
    posts: BigUpdateInfoRow,
    replies_relations: BigUpdateInfoRow,
    reply_to_relations: BigUpdateInfoRow,
    posts_relations: BigUpdateInfoRow,
    overwrite_latest_backfills: BigUpdateInfoRow,
}

impl BigUpdateInfo {
    fn all_relations(&self) -> BigUpdateInfoRow {
        BigUpdateInfoRow {
            count: self.likes.count
                + self.reposts.count
                + self.blocks.count
                + self.listblocks.count
                + self.listitems.count
                + self.replies_relations.count
                + self.reply_to_relations.count
                + self.posts_relations.count
                + self.quotes.count
                + self.follows.count,
            size: self.likes.size
                + self.reposts.size
                + self.blocks.size
                + self.listblocks.size
                + self.listitems.size
                + self.replies_relations.size
                + self.reply_to_relations.size
                + self.posts_relations.size
                + self.quotes.size
                + self.follows.size,
        }
    }
    fn all_tables(&self) -> BigUpdateInfoRow {
        BigUpdateInfoRow {
            count: self.did.count
                + self.feeds.count
                + self.lists.count
                + self.threadgates.count
                + self.starterpacks.count
                + self.postgates.count
                + self.actordeclarations.count
                + self.labelerservices.count
                + self.posts.count,
            size: self.did.size
                + self.feeds.size
                + self.lists.size
                + self.threadgates.size
                + self.starterpacks.size
                + self.postgates.size
                + self.actordeclarations.size
                + self.labelerservices.size
                + self.posts.size,
        }
    }
    fn all(&self) -> BigUpdateInfoRow {
        BigUpdateInfoRow {
            count: self.all_relations().count + self.all_tables().count,
            size: self.all_relations().size + self.all_tables().size,
        }
    }

    fn record_metrics(&self, source: &str) {
        INSERTED_ROWS_METRIC.add(
            self.all().count,
            &[KeyValue::new("source", source.to_string())],
        );
        INSERTED_SIZE_METRIC.add(
            self.all().size,
            &[KeyValue::new("source", source.to_string())],
        );
        TRANSACTIONS_METRIC.add(1, &[]);
    }
}

impl core::fmt::Debug for BigUpdateInfo {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_map()
            .entry(&"did", &self.did)
            .entry(&"follows", &self.follows)
            .entry(&"latest_backfills", &self.latest_backfills)
            .entry(&"likes", &self.likes)
            .entry(&"reposts", &self.reposts)
            .entry(&"blocks", &self.blocks)
            .entry(&"listblocks", &self.listblocks)
            .entry(&"listitems", &self.listitems)
            .entry(&"feeds", &self.feeds)
            .entry(&"lists", &self.lists)
            .entry(&"threadgates", &self.threadgates)
            .entry(&"starterpacks", &self.starterpacks)
            .entry(&"postgates", &self.postgates)
            .entry(&"actordeclarations", &self.actordeclarations)
            .entry(&"labelerservices", &self.labelerservices)
            .entry(&"quotes", &self.quotes)
            .entry(&"posts", &self.posts)
            .entry(&"replies_relations", &self.replies_relations)
            .entry(&"reply_to_relations", &self.reply_to_relations)
            .entry(&"posts_relations", &self.posts_relations)
            .entry(
                &"overwrite_latest_backfills",
                &self.overwrite_latest_backfills,
            )
            .finish()
    }
}

#[derive(Default)]
pub struct BigUpdate {
    /// Insert into did
    did: Vec<UpdateDid>,
    follows: Vec<UpdateFollow>,
    latest_backfills: Vec<UpdateLatestBackfill>,
    /// Like latest_backfills but overwrites existing records
    overwrite_latest_backfills: Vec<UpdateLatestBackfill>,
    likes: Vec<UpdateLike>,
    reposts: Vec<UpdateRepost>,
    blocks: Vec<UpdateBlock>,
    listblocks: Vec<UpdateListBlock>,
    listitems: Vec<UpdateListItem>,
    feeds: Vec<UpdateFeed>,
    lists: Vec<UpdateList>,
    threadgates: Vec<WithId<Box<Object<atrium_api::app::bsky::feed::threadgate::RecordData>>>>,
    starterpacks: Vec<WithId<Box<Object<atrium_api::app::bsky::graph::starterpack::RecordData>>>>,
    postgates: Vec<WithId<Box<Object<atrium_api::app::bsky::feed::postgate::RecordData>>>>,
    actordeclarations:
        Vec<WithId<Box<Object<atrium_api::chat::bsky::actor::declaration::RecordData>>>>,
    labelerservices: Vec<WithId<Box<Object<atrium_api::app::bsky::labeler::service::RecordData>>>>,
    quotes: Vec<UpdateQuote>,
    posts: Vec<WithId<BskyPost>>,
    replies_relations: Vec<UpdateRepliesRelation>,
    reply_to_relations: Vec<UpdateReplyToRelation>,
    posts_relations: Vec<UpdatePostsRelation>,
}
impl BigUpdate {
    pub fn merge(&mut self, other: BigUpdate) {
        self.did.extend(other.did);
        self.follows.extend(other.follows);
        self.latest_backfills.extend(other.latest_backfills);
        self.likes.extend(other.likes);
        self.reposts.extend(other.reposts);
        self.blocks.extend(other.blocks);
        self.listblocks.extend(other.listblocks);
        self.listitems.extend(other.listitems);
        self.feeds.extend(other.feeds);
        self.lists.extend(other.lists);
        self.threadgates.extend(other.threadgates);
        self.starterpacks.extend(other.starterpacks);
        self.postgates.extend(other.postgates);
        self.actordeclarations.extend(other.actordeclarations);
        self.labelerservices.extend(other.labelerservices);
        self.quotes.extend(other.quotes);
        self.posts.extend(other.posts);
        self.replies_relations.extend(other.replies_relations);
        self.reply_to_relations.extend(other.reply_to_relations);
        self.posts_relations.extend(other.posts_relations);
        self.overwrite_latest_backfills
            .extend(other.overwrite_latest_backfills);
    }

    pub fn add_timestamp(&mut self, did: &str, time: surrealdb::sql::Datetime) {
        self.overwrite_latest_backfills.push(UpdateLatestBackfill {
            of: RecordId::from(("did", did)),
            id: did.to_string(),
            at: Some(time),
        });
    }

    /// Apply this update to the database
    ///
    /// `source` is a string describing the source of the update, used for metrics
    pub async fn apply(self, db: &Surreal<Any>, source: &str) -> Result<()> {
        let start = Instant::now();
        // Convert the update to a string for logging later
        let info = tokio::task::block_in_place(|| self.create_info());

        // Create the query string
        let query_string = r#"
            BEGIN;
            INSERT IGNORE INTO did $dids RETURN NONE;
            INSERT IGNORE INTO latest_backfill $latest_backfills RETURN NONE;
            INSERT IGNORE INTO feed $feeds RETURN NONE;
            INSERT IGNORE INTO list $lists RETURN NONE;
            INSERT IGNORE INTO lex_app_bsky_feed_threadgate $threadgates RETURN NONE;
            INSERT IGNORE INTO lex_app_bsky_graph_starterpack $starterpacks RETURN NONE;
            INSERT IGNORE INTO lex_app_bsky_feed_postgate $postgates RETURN NONE;
            INSERT IGNORE INTO lex_chat_bsky_actor_declaration $actordeclarations RETURN NONE;
            INSERT IGNORE INTO lex_app_bsky_labeler_service $labelerservices RETURN NONE;
            INSERT IGNORE INTO posts $posts RETURN NONE;
            INSERT RELATION INTO quotes $quotes RETURN NONE;
            INSERT RELATION INTO like $likes RETURN NONE;
            INSERT RELATION INTO repost $reposts RETURN NONE;
            INSERT RELATION INTO block $blocks RETURN NONE;
            INSERT RELATION INTO listblock $listblocks RETURN NONE;
            INSERT RELATION INTO listitem $listitems RETURN NONE;
            INSERT RELATION INTO replyto $reply_to_relations RETURN NONE;
            INSERT RELATION INTO quotes $quotes RETURN NONE;
            INSERT RELATION INTO replies $replies_relations RETURN NONE;
            INSERT RELATION INTO follow $follows RETURN NONE;
            INSERT INTO latest_backfill $overwrite_latest_backfill RETURN NONE;
            COMMIT;
        "#;

        // Create the update query. Does not take that long; ~50ms for 30000 rows
        let update = tokio::task::block_in_place(|| {
            db.query(query_string)
                .bind(("dids", self.did))
                .bind(("follows", self.follows))
                .bind(("latest_backfills", self.latest_backfills))
                .bind(("likes", self.likes))
                .bind(("reposts", self.reposts))
                .bind(("blocks", self.blocks))
                .bind(("listblocks", self.listblocks))
                .bind(("listitems", self.listitems))
                .bind(("feeds", self.feeds))
                .bind(("lists", self.lists))
                .bind(("threadgates", self.threadgates))
                .bind(("starterpacks", self.starterpacks))
                .bind(("postgates", self.postgates))
                .bind(("actordeclarations", self.actordeclarations))
                .bind(("labelerservices", self.labelerservices))
                .bind(("quotes", self.quotes))
                .bind(("posts", self.posts))
                .bind(("replies_relations", self.replies_relations))
                .bind(("reply_to_relations", self.reply_to_relations))
                .bind(("posts_relations", self.posts_relations))
                .bind(("overwrite_latest_backfill", self.overwrite_latest_backfills))
                .into_future()
                .instrument(span!(Level::INFO, "query"))
        });

        let preparation_duration = start.elapsed();
        let after_update = Instant::now();
        update.await?;
        let update_duration = after_update.elapsed();
        QUERY_DURATION_METRIC.record(update_duration.as_millis() as u64, &[]);
        info.record_metrics(source);

        trace!(
            "Applied updated: {} elements, {}MB, {:03}ms preparation, {:03}ms applying",
            info.all().count,
            info.all().size as f64 / 1024.0 / 1024.0,
            preparation_duration.as_millis(),
            update_duration.as_millis(),
        );
        debug!("Detailed infos: {:?}", info);

        Ok(())
    }

    fn create_info(&self) -> BigUpdateInfo {
        BigUpdateInfo {
            did: BigUpdateInfoRow {
                count: self.did.len() as u64,
                size: self
                    .did
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            follows: BigUpdateInfoRow {
                count: self.follows.len() as u64,
                size: self
                    .follows
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            latest_backfills: BigUpdateInfoRow {
                count: self.latest_backfills.len() as u64,
                size: self
                    .latest_backfills
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            likes: BigUpdateInfoRow {
                count: self.likes.len() as u64,
                size: self
                    .likes
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            reposts: BigUpdateInfoRow {
                count: self.reposts.len() as u64,
                size: self
                    .reposts
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            blocks: BigUpdateInfoRow {
                count: self.blocks.len() as u64,
                size: self
                    .blocks
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            listblocks: BigUpdateInfoRow {
                count: self.listblocks.len() as u64,
                size: self
                    .listblocks
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            listitems: BigUpdateInfoRow {
                count: self.listitems.len() as u64,
                size: self
                    .listitems
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            feeds: BigUpdateInfoRow {
                count: self.feeds.len() as u64,
                size: self
                    .feeds
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            lists: BigUpdateInfoRow {
                count: self.lists.len() as u64,
                size: self
                    .lists
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            threadgates: BigUpdateInfoRow {
                count: self.threadgates.len() as u64,
                size: self
                    .threadgates
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            starterpacks: BigUpdateInfoRow {
                count: self.starterpacks.len() as u64,
                size: self
                    .starterpacks
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            postgates: BigUpdateInfoRow {
                count: self.postgates.len() as u64,
                size: self
                    .postgates
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            actordeclarations: BigUpdateInfoRow {
                count: self.actordeclarations.len() as u64,
                size: self
                    .actordeclarations
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            labelerservices: BigUpdateInfoRow {
                count: self.labelerservices.len() as u64,
                size: self
                    .labelerservices
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            quotes: BigUpdateInfoRow {
                count: self.quotes.len() as u64,
                size: self
                    .quotes
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            posts: BigUpdateInfoRow {
                count: self.posts.len() as u64,
                size: self
                    .posts
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            replies_relations: BigUpdateInfoRow {
                count: self.replies_relations.len() as u64,
                size: self
                    .replies_relations
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            reply_to_relations: BigUpdateInfoRow {
                count: self.reply_to_relations.len() as u64,
                size: self
                    .reply_to_relations
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            posts_relations: BigUpdateInfoRow {
                count: self.posts_relations.len() as u64,
                size: self
                    .posts_relations
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            overwrite_latest_backfills: BigUpdateInfoRow {
                count: self.overwrite_latest_backfills.len() as u64,
                size: self
                    .overwrite_latest_backfills
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
        }
    }
}

impl core::fmt::Debug for BigUpdate {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let info = self.create_info();
        info.fmt(f)
    }
}

/// If the new commit is a create or update, handle it
#[instrument(skip(record))]
pub fn create_big_update(
    did: Did,
    did_key: String,
    collection: String,
    rkey: RecordKey,
    record: KnownRecord,
) -> Result<BigUpdate> {
    utils::ensure_valid_rkey(rkey.to_string())?;

    let mut big_update = BigUpdate::default();

    match record {
        KnownRecord::AppBskyActorProfile(d) => {
            // NOTE: using .ok() here isn't optimal, incorrect data should
            // probably not be entered into the database at all, but for now
            // we'll just ignore it.
            let profile = UpdateDid {
                id: did_key.clone(),
                display_name: d.display_name.clone(),
                description: d.description.clone(),
                avatar: None, // TODO Implement
                banner: None, // TODO Implement
                created_at: d
                    .created_at
                    .as_ref()
                    .and_then(|dt| utils::extract_dt(dt).ok()),
                seen_at: Utc::now().into(),
                joined_via_starter_pack: d
                    .joined_via_starter_pack
                    .as_ref()
                    .and_then(|d| utils::strong_ref_to_record_id(d).ok()),
                // TODO if strong_ref_to_record_id fails, it should return an error result instead of being empty
                pinned_post: d
                    .pinned_post
                    .as_ref()
                    .and_then(|d| utils::strong_ref_to_record_id(d).ok()),
                labels: d
                    .labels
                    .as_ref()
                    .and_then(|d| utils::extract_self_labels_profile(d)),
                extra_data: process_extra_data(&d.extra_data)?,
            };
            big_update.did.push(profile);
        }
        KnownRecord::AppBskyGraphFollow(d) => {
            // TODO ensure_valid_rkey_strict(rkey.as_str())?;
            let from = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), from);
            let to = utils::did_to_key(d.subject.as_str())?;
            let created_at = utils::extract_dt(&d.created_at)?;

            big_update.follows.push(UpdateFollow {
                from: RecordId::from(("did", from)),
                to: RecordId::from(("did", to.clone())),
                id: id,
                created_at,
            });

            big_update.latest_backfills.push(UpdateLatestBackfill {
                of: RecordId::from(("did", to.clone())),
                id: to,
                at: None,
            });
        }
        KnownRecord::AppBskyFeedLike(d) => {
            // TODO ensure_valid_rkey_strict(rkey.as_str())?;
            let from = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), from);
            let to = utils::at_uri_to_record_id(&d.subject.uri)?;
            let created_at = utils::extract_dt(&d.created_at)?;

            big_update.likes.push(UpdateLike {
                from: RecordId::from(("did", from)),
                to: to,
                id: id,
                created_at,
            });
        }
        KnownRecord::AppBskyFeedRepost(d) => {
            // TODO ensure_valid_rkey_strict(rkey.as_str())?;
            let from = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), from);
            let to = utils::at_uri_to_record_id(&d.subject.uri)?;
            let created_at = utils::extract_dt(&d.created_at)?;

            big_update.reposts.push(UpdateRepost {
                from: RecordId::from(("did", from)),
                to: to,
                id: id,
                created_at,
            });
        }
        KnownRecord::AppBskyGraphBlock(d) => {
            // TODO ensure_valid_rkey_strict(rkey.as_str())?;
            let from = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), from);
            let to = utils::did_to_key(d.subject.as_str())?;
            let created_at = utils::extract_dt(&d.created_at)?;

            big_update.blocks.push(UpdateBlock {
                from: RecordId::from(("did", from)),
                to: RecordId::from(("did", to.clone())),
                id: id,
                created_at,
            });
        }
        KnownRecord::AppBskyGraphListblock(d) => {
            // TODO ensure_valid_rkey_strict(rkey.as_str())?;
            let from = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), from);
            let to = utils::at_uri_to_record_id(&d.subject)?;
            let created_at = utils::extract_dt(&d.created_at)?;

            big_update.listblocks.push(UpdateListBlock {
                from: RecordId::from(("did", from)),
                to: to,
                id: id,
                created_at,
            });
        }
        KnownRecord::AppBskyGraphListitem(d) => {
            // TODO ensure_valid_rkey_strict(rkey.as_str())?;
            let from = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), from);

            let from = utils::at_uri_to_record_id(&d.list)?;
            let to = utils::did_to_key(&d.subject)?;
            let created_at = utils::extract_dt(&d.created_at)?;

            big_update.listitems.push(UpdateListItem {
                from: from,
                to: RecordId::from(("did", to.clone())),
                id: id,
                created_at,
            });
        }
        KnownRecord::AppBskyFeedGenerator(d) => {
            let did_key = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), did_key);
            let feed = UpdateFeed {
                id: id,
                author: RecordId::from_table_key("did", did_key),
                avatar: None, // TODO implement
                created_at: utils::extract_dt(&d.created_at)?,
                description: d.description.clone(),
                did: d.did.to_string(),
                display_name: d.display_name.clone(),
                rkey: rkey.to_string(),
                uri: format!(
                    "at://{}/app.bsky.feed.generator/{}",
                    did.as_str(),
                    rkey.as_str()
                ),
                extra_data: process_extra_data(&d.extra_data)?,
            };
            big_update.feeds.push(feed);
        }
        KnownRecord::AppBskyGraphList(d) => {
            let did_key = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), did_key);

            let list = UpdateList {
                id: id,
                name: d.name.clone(),
                avatar: None, // TODO implement
                created_at: utils::extract_dt(&d.created_at)?,
                description: d.description.clone(),
                labels: d
                    .labels
                    .as_ref()
                    .and_then(|d| utils::extract_self_labels_list(d)),
                purpose: d.purpose.clone(),
                extra_data: process_extra_data(&d.extra_data)?,
            };
            big_update.lists.push(list);
        }
        KnownRecord::AppBskyFeedThreadgate(d) => {
            let did_key = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), did_key);
            big_update.threadgates.push(WithId { id: id, data: d });
        }
        KnownRecord::AppBskyGraphStarterpack(d) => {
            let did_key = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), did_key);
            big_update.starterpacks.push(WithId { id: id, data: d });
        }
        KnownRecord::AppBskyFeedPostgate(d) => {
            let did_key = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), did_key);
            big_update.postgates.push(WithId { id: id, data: d });
        }
        KnownRecord::ChatBskyActorDeclaration(d) => {
            let did_key = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), did_key);
            big_update
                .actordeclarations
                .push(WithId { id: id, data: d });
        }
        KnownRecord::AppBskyLabelerService(d) => {
            let did_key = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), did_key);
            big_update.labelerservices.push(WithId { id: id, data: d });
        }
        KnownRecord::AppBskyFeedPost(d) => {
            let did_key = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), did_key);

            let mut images: Vec<BskyPostImage> = vec![];
            let mut links: Vec<String> = vec![];
            let mut mentions: Vec<RecordId> = vec![];
            let mut record: Option<RecordId> = None;
            let mut tags: Vec<String> = vec![];
            let mut video: Option<BskyPostVideo> = None;

            let mut post_images: Vec<atrium_api::app::bsky::embed::images::Image> = vec![];

            match &d.embed {
                Some(d) => {
                    match d {
                        atrium_api::types::Union::Refs(e) => {
                            match e {
                          atrium_api::app::bsky::feed::post::RecordEmbedRefs::AppBskyEmbedExternalMain(m)=>{
                            // TODO index preview too
                            links.push(m.external.uri.clone());
                          },
                            atrium_api::app::bsky::feed::post::RecordEmbedRefs::AppBskyEmbedImagesMain(m) => {
                              post_images=m.images.clone();
                            },
                            atrium_api::app::bsky::feed::post::RecordEmbedRefs::AppBskyEmbedVideoMain(m) => {
                              video = Some(process_video(m)?);
                            },
                            atrium_api::app::bsky::feed::post::RecordEmbedRefs::AppBskyEmbedRecordMain(m) => {
                              record = Some(at_uri_to_record_id(&m.record.uri)?);
                            },
                            atrium_api::app::bsky::feed::post::RecordEmbedRefs::AppBskyEmbedRecordWithMediaMain(m) => {
                              record = Some(at_uri_to_record_id(&m.record.record.uri)?);

                              match &m.media{
                                atrium_api::types::Union::Refs(r)=>match r{
                                  atrium_api::app::bsky::embed::record_with_media::MainMediaRefs::AppBskyEmbedExternalMain(m)=>{
                                    // TODO index preview too
                                    links.push(m.external.uri.clone());
                                  }
                                  atrium_api::app::bsky::embed::record_with_media::MainMediaRefs::AppBskyEmbedImagesMain(m)=>{
                                    post_images=m.images.clone();
                                  }
                                  atrium_api::app::bsky::embed::record_with_media::MainMediaRefs::AppBskyEmbedVideoMain(m)=>{

                                    video = Some(process_video(m)?);
                                  }
                                }
                                atrium_api::types::Union::Unknown(_)=>{}
                              }
                            },
                        }
                        }
                        atrium_api::types::Union::Unknown(_) => {}
                    }
                }
                None => {}
            };

            if !post_images.is_empty() {
                for i in post_images {
                    images.push(BskyPostImage {
                        alt: i.alt.clone(),
                        blob: blob_ref_to_record_id(&i.image), // TODO store blob details
                        aspect_ratio: i.aspect_ratio.as_ref().map(|a| BskyPostMediaAspectRatio {
                            height: a.height.into(),
                            width: a.width.into(),
                        }),
                    })
                }
            }

            if let Some(facets) = &d.facets {
                for facet in facets {
                    for feature in &facet.features {
                        match feature {
                            atrium_api::types::Union::Refs(refs) => match refs {
                                MainFeaturesItem::Mention(m) => {
                                    mentions.push(("did", did_to_key(m.did.as_str())?).into());
                                }
                                MainFeaturesItem::Link(l) => {
                                    links.push(l.uri.clone());
                                }
                                MainFeaturesItem::Tag(t) => {
                                    tags.push(t.tag.clone());
                                }
                            },
                            atrium_api::types::Union::Unknown(_) => {}
                        }
                    }
                }
            }

            if let Some(t) = &d.tags {
                tags.extend(t.clone());
            }

            if let Some(r) = &record {
                if r.table() == "post" {
                    big_update.quotes.push(UpdateQuote {
                        from: RecordId::from_table_key("post", id.clone()),
                        to: r.clone(),
                        id: id.clone(),
                    });
                }
            }

            let post = WithId {
                id: id.clone(),
                data: BskyPost {
                    author: RecordId::from_table_key("did", did_key.clone()),
                    bridgy_original_url: None,
                    via: None,
                    created_at: utils::extract_dt(&d.created_at)?,
                    labels: d
                        .labels
                        .as_ref()
                        .and_then(|d| utils::extract_self_labels_post(d)),
                    text: d.text.clone(),
                    langs: d
                        .langs
                        .as_ref()
                        .map(|d| d.iter().map(|l| l.as_ref().to_string()).collect()),
                    root: d
                        .reply
                        .as_ref()
                        .map(|r| utils::strong_ref_to_record_id(&r.root))
                        .transpose()?,
                    parent: d
                        .reply
                        .as_ref()
                        .map(|r| utils::strong_ref_to_record_id(&r.parent))
                        .transpose()?,
                    video: video,
                    tags: if tags.is_empty() { None } else { Some(tags) },
                    links: if links.is_empty() { None } else { Some(links) },
                    mentions: if mentions.is_empty() {
                        None
                    } else {
                        Some(mentions)
                    },
                    record: record,
                    images: if images.is_empty() {
                        None
                    } else {
                        Some(images)
                    },
                    extra_data: process_extra_data(&d.extra_data)?,
                },
            };

            let parent = post.data.parent.clone();
            big_update.posts.push(post);

            if parent.is_some() {
                big_update.replies_relations.push(UpdateRepliesRelation {
                    from: RecordId::from_table_key("did", did_key.clone()),
                    to: RecordId::from_table_key("post", id.clone()),
                    id: id.clone(),
                });

                big_update.reply_to_relations.push(UpdateReplyToRelation {
                    from: RecordId::from_table_key("post", id.clone()),
                    to: parent.unwrap(),
                    id: id.clone(),
                });
            } else {
                big_update.posts_relations.push(UpdatePostsRelation {
                    from: RecordId::from_table_key("did", did_key.clone()),
                    to: RecordId::from_table_key("post", id.clone()),
                    id: id.clone(),
                });
            }
        }
        _ => {
            warn!(target: "indexer", "ignored create_or_update {} {} {}",
                did.as_str(), collection, rkey.as_str());
        }
    }

    Ok(big_update)
}

fn process_video(vid: &video::Main) -> Result<BskyPostVideo> {
    let blob = extract_video_blob(&vid.video)?;
    let v = BskyPostVideo {
        alt: vid.alt.clone(),
        aspect_ratio: vid.aspect_ratio.clone().map(|a| BskyPostMediaAspectRatio {
            height: a.height.into(),
            width: a.width.into(),
        }),
        blob: BskyPostVideoBlob {
            cid: blob.r#ref.0.to_string(),
            media_type: blob.mime_type,
            size: blob.size as u64,
        },
        captions: None, // TODO implement
    };
    Ok(v)
}
fn extract_video_blob(blob: &BlobRef) -> Result<Blob> {
    match blob {
        atrium_api::types::BlobRef::Typed(a) => match a {
            atrium_api::types::TypedBlobRef::Blob(b) => Ok(b.clone()),
        },
        atrium_api::types::BlobRef::Untyped(_) => anyhow::bail!("Invalid blob ref type"),
    }
}

fn process_extra_data(ipld: &ipld_core::ipld::Ipld) -> Result<Option<String>> {
    let str = simd_json::serde::to_string(ipld)?;
    Ok(if str == "{}" { None } else { Some(str) })
}
