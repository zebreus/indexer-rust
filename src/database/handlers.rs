use super::big_update::create_big_update;
use super::{
    definitions::{JetstreamAccountEvent, JetstreamIdentityEvent, Record},
    delete_record, utils,
};
use crate::websocket::events::{Commit, Kind};
use anyhow::Result;
use atrium_api::types::string::{Did, RecordKey};
use surrealdb::{engine::any::Any, Surreal};
use tracing::warn;

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
                    collection,
                    rkey,
                    record,
                    ..
                } => {
                    let big_update = create_big_update(did, did_key, collection, rkey, record)?;
                    big_update.apply(db, "jetstream").await?;
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
