use crate::database::utils::unsafe_user_key_to_did;
use anyhow::{anyhow, Context};
use async_channel::{Receiver, Sender};
use index_repo::index_repo;
use log::{error, info, warn};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, sync::Arc, time::Duration};
use surrealdb::{engine::any::Any, Surreal};
use tokio::time::sleep;

mod index_repo;

#[derive(Deserialize)]
struct BskyFollowRes {
    #[serde(rename = "in")]
    pub from: surrealdb::RecordId,
    #[serde(rename = "out")]
    pub to: surrealdb::RecordId,
    pub id: surrealdb::RecordId,
}

#[derive(Debug)]
pub struct SharedState {
    rx: Receiver<String>,
    db: Surreal<Any>,
    http_client: Client,
}

/// Database struct for a repo indexing timestamp
#[derive(Debug, Serialize, Deserialize)]
pub struct LastIndexedTimestamp {
    pub time_us: u64,
    pub time_dt: surrealdb::Datetime,
    pub error: Option<String>,
}

/// Size of the queue of discovered repos
const QUEUE_SIZE: usize = 200;
/// How long to sleep, when the discovery queue is full
const DISCOVERY_QUEUE_FULL_BACKOFF: Duration = Duration::from_millis(500);
/// How long to sleep, when the we caught up with discovery
const DISCOVERY_CAUGHT_UP_BACKOFF: Duration = Duration::from_millis(500);
/// An ID that was used before the earliest data we are interested in
const OLDEST_USEFUL_ANCHOR: &str = "3juj4";
/// How long to sleep, when the queue is empty
const FETCHER_BACKOFF: Duration = Duration::from_millis(500);

pub async fn start_full_repo_indexer(db: Surreal<Any>, max_tasks: usize) -> anyhow::Result<()> {
    let (tx, rx) = async_channel::bounded(QUEUE_SIZE);
    let state = Arc::new(SharedState {
        rx,
        db,
        http_client: Client::new(),
    });

    info!(target: "indexer", "Spinning up {} handler tasks", max_tasks);

    for thread_id in 0..max_tasks {
        let state = state.clone();
        tokio::spawn(async move {
            let result = repo_fetcher_task(state).await;
            let error = result
                .and::<()>(Err(anyhow!("Handler thread should never exit")))
                .unwrap_err();
            error!(target: "indexer", "Handler thread {} failed: {:?}", thread_id, error);
        });
    }

    repo_discovery_task(state, tx).await.unwrap();

    Ok(())
}

async fn repo_fetcher_task(state: Arc<SharedState>) -> anyhow::Result<()> {
    loop {
        let Ok(did) = state.rx.try_recv() else {
            sleep(FETCHER_BACKOFF).await;
            continue;
        };

        let result = index_repo(&state, &did).await;

        if let Err(error) = result {
            warn!(target: "indexer", "Failed to index repo {}: {}", did, error);

            let error_message = format!("{}", error);
            if format!("{}", error) == "Failed to parse CAR file: early eof" {
                // TODO: Document what this case does

                let did_key = crate::database::utils::did_to_key(did.as_str())?;
                let timestamp_us = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_micros();
                let _: Option<super::definitions::Record> = state
                    .db
                    .upsert(("li_did", did_key))
                    .content(LastIndexedTimestamp {
                        time_us: timestamp_us as u64,
                        time_dt: chrono::Utc::now().into(),
                        error: Some(error_message),
                    })
                    .await?;
            }
        }
    }
}

async fn repo_discovery_task(state: Arc<SharedState>, tx: Sender<String>) -> anyhow::Result<()> {
    // Max number of items in the queue
    let queue_capacity = tx.capacity().unwrap_or(QUEUE_SIZE);
    assert!(queue_capacity > 3);
    // Start fetching when the queue is emptier than this
    let fetch_threshold = (((queue_capacity / 3) * 2) - 1).max(1);
    // How many items to fetch at once
    let fetch_amount = (queue_capacity / 3).max(1);
    // Start printing a warning when the queue is too empty
    let warning_threshold = queue_capacity / 3;

    let mut processed_dids: HashSet<String> = HashSet::new();
    let mut anchor = OLDEST_USEFUL_ANCHOR.to_string();
    loop {
        if tx.len() > fetch_threshold {
            sleep(DISCOVERY_QUEUE_FULL_BACKOFF).await;
            continue;
        }

        info!(target: "indexer", "Discovering follows starting from {}", anchor);
        let mut result = state
            .db
            // TODO: Fix the possible SQL injection
            .query(format!(
                "SELECT id,in,out FROM follow:{}.. LIMIT {};",
                anchor, fetch_amount
            ))
            .await?;
        let follows: Vec<BskyFollowRes> = result.take(0)?;

        let Some(anchor_key) = follows.last().map(|follow| follow.id.key()) else {
            sleep(DISCOVERY_CAUGHT_UP_BACKOFF).await;
            continue;
        };
        anchor = format!("{}", anchor_key);

        let processed_dids_before = processed_dids.len();
        for follow in &follows {
            for record_id in [&follow.from, &follow.to] {
                let did = unsafe_user_key_to_did(&format!("{}", record_id.key()));
                if processed_dids.contains(&did) {
                    continue;
                }
                processed_dids.insert(did.clone());
                tx.send(did)
                    .await
                    .context("Failed to send message to handler thread")?;
            }
        }

        // Warn if it looks like the queue size or the backoff were choosen incorrectly
        let new_follows = processed_dids.len() - processed_dids_before;
        if new_follows != 0 && follows.len() == fetch_amount && tx.len() < warning_threshold {
            warn!(target: "indexer", "Queue is not getting filled up fast enough. Consider increasing the queue size or decreasing the backoff.");
        }
    }
}
