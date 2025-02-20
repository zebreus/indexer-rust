use crate::database::utils::unsafe_user_key_to_did;
use anyhow::{anyhow, Context};
use async_channel::{Receiver, Sender};
use index_repo::index_repo;
use log::{error, info, warn};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, sync::Arc};
use surrealdb::{engine::any::Any, Surreal};

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

pub async fn start_full_repo_indexer(db: Surreal<Any>, max_tasks: usize) -> anyhow::Result<()> {
    let (tx, rx) = async_channel::bounded(10_000);

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
        let did = state.rx.recv().await.unwrap();

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
    // An ID that was used before the earliest data we are interested in
    const OLDEST_USEFUL_ANCHOR: &str = "3juj4";

    let mut processed_dids: BTreeSet<String> = BTreeSet::new();
    let mut anchor = OLDEST_USEFUL_ANCHOR.to_string();
    loop {
        info!(target: "indexer", "anchor {}", anchor);

        let mut result = state
            .db
            .query(format!(
                "SELECT id,in,out FROM follow:{}.. LIMIT 500000;",
                anchor
            ))
            .await?;
        let follows: Vec<BskyFollowRes> = result.take(0)?;

        if follows.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(10000)).await;
            continue;
        }

        anchor = format!("{}", follows.last().unwrap().id.key());

        let mut dids: BTreeSet<String> = BTreeSet::new();

        for follow in &follows {
            for record_id in [&follow.from, &follow.to] {
                let did = unsafe_user_key_to_did(&format!("{}", record_id.key()));
                if processed_dids.contains(&did) {
                    continue;
                }
                dids.insert(did.clone());
                processed_dids.insert(did);
            }
        }

        for did in dids {
            tx.send(did)
                .await
                .context("Failed to send message to handler thread")?;
        }

        // TODO: Remove and add proper backpressure
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}
