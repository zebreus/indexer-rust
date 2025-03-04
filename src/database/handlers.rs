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
use serde::{Deserialize, Serialize};
use std::future::IntoFuture;
use std::time::Instant;
use surrealdb::method::Query;
use surrealdb::Datetime;
use surrealdb::{engine::any::Any, RecordId, Surreal};
use tracing::{instrument, span, warn, Instrument, Level};

use crate::websocket::events::{Commit, Kind};

use super::{
    definitions::{
        BskyFeed, BskyList, BskyPost, BskyPostImage, BskyPostMediaAspectRatio, BskyPostVideo,
        BskyPostVideoBlob, BskyProfile, JetstreamAccountEvent, JetstreamIdentityEvent, Record,
    },
    delete_record,
    utils::{self, at_uri_to_record_id, blob_ref_to_record_id, did_to_key},
};

/// Handle a new websocket event on the database
pub async fn handle_event(db: &Surreal<Any>, event: Kind) -> Result<()> {
    // Handle event types
    match event {
        Kind::CommitEvent {
            did,
            time_us,
            commit,
        } => {
            // Handle types of commits
            let did_key = utils::did_to_key(did.as_str())?;
            match commit {
                Commit::CreateOrUpdate {
                    rev,
                    collection,
                    rkey,
                    record,
                    cid,
                } => {
                    let big_update =
                        on_commit_event_createorupdate(did, did_key, collection, rkey, record)?;
                    apply_big_update(db, big_update).await?;
                }
                Commit::Delete {
                    rev,
                    collection,
                    rkey,
                } => {
                    on_commit_event_delete(db, did, time_us, did_key, rev, collection, rkey).await?
                }
            }
        }
        Kind::IdentityEvent {
            did,
            time_us,
            identity,
        } => {
            let did_key = utils::did_to_key(did.as_str())?;
            let _: Option<Record> = db
                .upsert(("jetstream_identity", did_key))
                .content(JetstreamIdentityEvent {
                    time_us,
                    handle: identity.handle.to_string(),
                    seq: identity.seq,
                    time: identity.time,
                })
                .await?;
        }
        Kind::KeyEvent {
            did,
            time_us,
            account,
        } => {
            let did_key = utils::did_to_key(did.as_str())?;
            let _: Option<Record> = db
                .upsert(("jetstream_account", did_key))
                .content(JetstreamAccountEvent {
                    time_us,
                    active: account.active,
                    seq: account.seq,
                    time: account.time,
                })
                .await?;
        }
    }

    Ok(())
}

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

#[derive(Serialize)]
struct UpdateLatestBackfill {
    pub of: surrealdb::RecordId,
    pub id: String,
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

#[derive(Default)]
pub struct BigUpdate {
    /// Insert into did
    did: Vec<UpdateDid>,
    follows: Vec<UpdateFollow>,
    latest_backfills: Vec<UpdateLatestBackfill>,
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
    }
}

pub async fn apply_big_update(db: &Surreal<Any>, big_update: BigUpdate) -> Result<()> {
    //TODO: Bundle this into a function
    let query_string = r#"
        INSERT IGNORE INTO did $dids;
        INSERT RELATION INTO follow $follows;
        INSERT IGNORE INTO latest_backfill $latest_backfills;
        INSERT RELATION INTO like $likes;
        INSERT RELATION INTO repost $reposts;
        INSERT RELATION INTO block $blocks;
        INSERT RELATION INTO listblock $listblocks;
        INSERT RELATION INTO listitem $listitems;
        INSERT IGNORE INTO feed $feeds;
        INSERT IGNORE INTO list $lists;
        INSERT IGNORE INTO lex_app_bsky_feed_threadgate $threadgates;
        INSERT IGNORE INTO lex_app_bsky_graph_starterpack $starterpacks;
        INSERT IGNORE INTO lex_app_bsky_feed_postgate $postgates;
        INSERT IGNORE INTO lex_chat_bsky_actor_declaration $actordeclarations;
        INSERT IGNORE INTO lex_app_bsky_labeler_service $labelerservices;
        INSERT RELATION INTO quote $quotes;
        INSERT IGNORE INTO posts $posts;
        INSERT RELATION INTO replyto $reply_to_relations;
        INSERT RELATION INTO quotes $quotes;
        INSERT RELATION INTO replies $replies_relations;
    "#;

    let start = Instant::now();
    db.query(query_string)
        .bind(("dids", big_update.did))
        .bind(("follows", big_update.follows))
        .bind(("latest_backfills", big_update.latest_backfills))
        .bind(("likes", big_update.likes))
        .bind(("reposts", big_update.reposts))
        .bind(("blocks", big_update.blocks))
        .bind(("listblocks", big_update.listblocks))
        .bind(("listitems", big_update.listitems))
        .bind(("feeds", big_update.feeds))
        .bind(("lists", big_update.lists))
        .bind(("threadgates", big_update.threadgates))
        .bind(("starterpacks", big_update.starterpacks))
        .bind(("postgates", big_update.postgates))
        .bind(("actordeclarations", big_update.actordeclarations))
        .bind(("labelerservices", big_update.labelerservices))
        .bind(("quotes", big_update.quotes))
        .bind(("posts", big_update.posts))
        .bind(("replies_relations", big_update.replies_relations))
        .bind(("reply_to_relations", big_update.reply_to_relations))
        .bind(("posts_relations", big_update.posts_relations))
        .into_future()
        .instrument(span!(Level::INFO, "query"))
        .await?;
    let duration = start.elapsed();
    warn!("Big update took {:?}", duration);

    Ok(())
}

/// If the new commit is a create or update, handle it
#[instrument(skip(record))]
pub fn on_commit_event_createorupdate(
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
            // // TODO this should be a db.upsert(...).merge(...)
            // let _: Option<Record> = db
            //     .insert(("did", did_key))
            //     .content(profile)
            //     .into_future()
            //     .instrument(span!(Level::INFO, "upsert"))
            //     .await?;
        }
        KnownRecord::AppBskyGraphFollow(d) => {
            // TODO ensure_valid_rkey_strict(rkey.as_str())?;
            let from = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), from);
            let to = utils::did_to_key(d.subject.as_str())?;
            let created_at = utils::extract_dt(&d.created_at)?;

            // let query = format!(
            //     r#"RELATE type::thing("did", $from)->follow->type::thing("did", $to) SET id = $id, createdAt = $created_at;"#
            //     from, to, id, created_at
            // );

            big_update.follows.push(UpdateFollow {
                from: RecordId::from(("did", from)),
                to: RecordId::from(("did", to.clone())),
                id: id,
                created_at,
            });

            big_update.latest_backfills.push(UpdateLatestBackfill {
                of: RecordId::from(("did", to.clone())),
                id: to,
            });

            // let _ = db
            // .query("RELATE (type::thing('did', $from))->follow->(type::thing('did', $to)) SET id = $id, createdAt = $created_at; UPSERT (type::thing('latest_backfill', $to)) SET of = type::thing('did', $to);")
            // .bind(("from", from))
            // .bind(("to", to))
            // .bind(("id", id))
            //   .     bind(("created_at", created_at))
            //     .into_future()
            //       .instrument(span!(Level::INFO, "query"))
            //     .await.unwrap();
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

            // let query = format!(
            //     "RELATE did:{}->like->{} SET id = '{}', createdAt = {};",
            //     from, to, id, created_at
            // );

            // let _ = db
            //     .query(query)
            //     .into_future()
            //     .instrument(span!(Level::INFO, "query"))
            //     .await?;
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
            // let query = format!(
            //     "RELATE did:{}->repost->{} SET id = '{}', createdAt = {};",
            //     from, to, id, created_at
            // );

            // let _ = db
            //     .query(query)
            //     .into_future()
            //     .instrument(span!(Level::INFO, "query"))
            //     .await?;
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
            // let query = format!(
            //     "RELATE did:{}->block->did:{} SET id = '{}', createdAt = {};",
            //     from, to, id, created_at
            // );

            // let _ = db
            //     .query(query)
            //     .into_future()
            //     .instrument(span!(Level::INFO, "query"))
            //     .await?;
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
            // let query = format!(
            //     "RELATE did:{}->listblock->{} SET id = '{}', createdAt = {};",
            //     from, to, id, created_at
            // );

            // let _ = db
            //     .query(query)
            //     .into_future()
            //     .instrument(span!(Level::INFO, "query"))
            //     .await?;
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

            // let query = format!(
            //     "RELATE {}->listitem->did:{} SET id = '{}', createdAt = {};",
            //     from, to, id, created_at
            // );

            // let _ = db
            //     .query(query)
            //     .into_future()
            //     .instrument(span!(Level::INFO, "query"))
            //     .await?;
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
            // let _: Option<Record> = db
            //     .upsert(("feed", id))
            //     .content(feed)
            //     .into_future()
            //     .instrument(span!(Level::INFO, "upsert"))
            //     .await?;
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
            // let _: Option<Record> = db
            //     .upsert(("list", id))
            //     .content(list)
            //     .into_future()
            //     .instrument(span!(Level::INFO, "upsert"))
            //     .await?;
        }
        KnownRecord::AppBskyFeedThreadgate(d) => {
            let did_key = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), did_key);
            big_update.threadgates.push(WithId { id: id, data: d });
            // let _: Option<Record> = db
            //     .upsert(("lex_app_bsky_feed_threadgate", id))
            //     .content(d)
            //     .into_future()
            //     .instrument(span!(Level::INFO, "upsert"))
            //     .await?;
        }
        KnownRecord::AppBskyGraphStarterpack(d) => {
            let did_key = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), did_key);
            big_update.starterpacks.push(WithId { id: id, data: d });
            // let _: Option<Record> = db
            //     .upsert(("lex_app_bsky_graph_starterpack", id))
            //     .content(d)
            //     .into_future()
            //     .instrument(span!(Level::INFO, "upsert"))
            //     .await?;
        }
        KnownRecord::AppBskyFeedPostgate(d) => {
            let did_key = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), did_key);
            big_update.postgates.push(WithId { id: id, data: d });
            // let _: Option<Record> = db
            //     .upsert(("lex_app_bsky_feed_postgate", id))
            //     .content(d)
            //     .into_future()
            //     .instrument(span!(Level::INFO, "upsert"))
            //     .await?;
        }
        KnownRecord::ChatBskyActorDeclaration(d) => {
            let did_key = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), did_key);
            big_update
                .actordeclarations
                .push(WithId { id: id, data: d });
            // let _: Option<Record> = db
            //     .upsert(("lex_chat_bsky_actor_declaration", id))
            //     .content(d)
            //     .into_future()
            //     .instrument(span!(Level::INFO, "upsert"))
            //     .await?;
        }
        KnownRecord::AppBskyLabelerService(d) => {
            let did_key = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), did_key);
            big_update.labelerservices.push(WithId { id: id, data: d });
            // let _: Option<Record> = db
            //     .upsert(("lex_app_bsky_labeler_service", id))
            //     .content(d)
            //     .into_future()
            //     .instrument(span!(Level::INFO, "upsert"))
            //     .await?;
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

                    // let query = format!(
                    //     "RELATE post:{}->quotes->post:{} SET id = '{}';",
                    //     id,
                    //     r.key(),
                    //     id
                    // );

                    // let _ = db
                    //     .query(query)
                    //     .into_future()
                    //     .instrument(span!(Level::INFO, "query"))
                    //     .await?;
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
            // let _: Option<Record> = db
            //     .upsert(("post", id.clone()))
            //     .content(post)
            //     .into_future()
            //     .instrument(span!(Level::INFO, "upsert"))
            //     .await?;

            if parent.is_some() {
                big_update.replies_relations.push(UpdateRepliesRelation {
                    from: RecordId::from_table_key("did", did_key.clone()),
                    to: RecordId::from_table_key("post", id.clone()),
                    id: id.clone(),
                });
                // let query1 = format!(
                //     "RELATE did:{}->replies->post:{} SET id = '{}';",
                //     did_key, id, id
                // );
                // let _ = db
                //     .query(query1)
                //     .into_future()
                //     .instrument(span!(Level::INFO, "query"))
                //     .await?;

                big_update.reply_to_relations.push(UpdateReplyToRelation {
                    from: RecordId::from_table_key("post", id.clone()),
                    to: parent.unwrap(),
                    id: id.clone(),
                });
                // let query2 = format!(
                //     "RELATE post:{}->replyto->{} SET id = '{}';",
                //     id,
                //     parent.unwrap(),
                //     id
                // );
                // let _ = db
                //     .query(query2)
                //     .into_future()
                //     .instrument(span!(Level::INFO, "query"))
                //     .await?;
            } else {
                big_update.posts_relations.push(UpdatePostsRelation {
                    from: RecordId::from_table_key("did", did_key.clone()),
                    to: RecordId::from_table_key("post", id.clone()),
                    id: id.clone(),
                });
                // let query = format!(
                //     "RELATE did:{}->posts->post:{} SET id = '{}';",
                //     did_key, id, id
                // );
                // let _ = db
                //     .query(query)
                //     .into_future()
                //     .instrument(span!(Level::INFO, "query"))
                //     .await?;
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

/// If the new commit is a delete, handle it
async fn on_commit_event_delete(
    db: &Surreal<Any>,
    did: Did,
    _time_us: u64,
    _did_key: String,
    _rev: String,
    collection: String,
    rkey: RecordKey,
) -> Result<()> {
    utils::ensure_valid_rkey(rkey.to_string())?;

    let id = format!("{}_{}", rkey.as_str(), utils::did_to_key(did.as_str())?);
    match collection.as_str() {
        "app.bsky.graph.follow" => {
            delete_record(db, "follow", &id).await?;
        }
        "app.bsky.feed.repost" => {
            delete_record(db, "repost", &id).await?;
        }
        "app.bsky.feed.like" => {
            delete_record(db, "like", &id).await?;
        }
        "app.bsky.graph.block" => {
            delete_record(db, "block", &id).await?;
        }
        "app.bsky.graph.listblock" => {
            delete_record(db, "listblock", &id).await?;
        }
        "app.bsky.feed.post" => {
            for table in vec!["post", "posts", "replies", "replyto", "quotes"] {
                delete_record(db, table, &id).await?;
            }
        }
        "app.bsky.graph.listitem" => {
            delete_record(db, "listitem", &id).await?;
        }
        "app.bsky.feed.threadgate" => {
            delete_record(db, "threadgate", &id).await?;
        }
        "app.bsky.feed.generator" => {
            delete_record(db, "feed", &id).await?;
        }
        "app.bsky.graph.list" => {
            delete_record(db, "list", &id).await?;
        }
        "app.bsky.feed.postgate" => {
            delete_record(db, "postgate", &id).await?;
        }
        "app.bsky.graph.starterpack" => {
            delete_record(db, "starterpack", &id).await?;
        }
        "app.bsky.labeler.service" => {
            delete_record(db, "labeler", &id).await?;
        }
        "chat.bsky.actor.declaration" => {
            delete_record(db, "chat_bsky_actor_declaration", &id).await?;
        }
        _ => {
            warn!(target: "indexer", "could not handle operation {} {} {} {}",
                did.as_str(), "delete", collection, rkey.as_str());
        }
    }

    Ok(())
}

fn process_extra_data(ipld: &ipld_core::ipld::Ipld) -> Result<Option<String>> {
    let str = simd_json::serde::to_string(ipld)?;
    Ok(if str == "{}" { None } else { Some(str) })
}
