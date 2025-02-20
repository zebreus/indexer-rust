use crate::database::utils::unsafe_user_key_to_did;
use anyhow::Context;
use index_repo::index_repo;
use log::{debug, error, info, warn};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, sync::Arc};
use surrealdb::{engine::any::Any, Surreal};

mod index_repo;

pub async fn start_full_repo_indexer(db: Surreal<Any>, max_tasks: usize) -> anyhow::Result<()> {
    let mut processed_dids: BTreeSet<String> = BTreeSet::new();
    let (tx, rx) = async_channel::bounded(10_000);

    let state = Arc::new(SharedState {
        rx,
        db,
        http_client: Client::new(),
    });

    info!(target: "indexer", "Spinning up {} handler tasks", max_tasks);

    for i in 0..max_tasks {
        let state = state.clone();
        tokio::spawn(async move {
            /*   thread::Builder::new()
            .name(format!("Indexer Thread {}", i))
            .spawn(move || {
                Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                    .unwrap()
                    .block_on(async { */
            let res = task_handler(state, i as u64).await;
            if let Err(e) = res {
                error!(target: "indexer", "Handler thread {} failed: {:?}", i, e);
            } else {
                debug!(target: "indexer", "Handler thread {} exited", i);
            }
        });
        /* })
        .context("Failed to spawn handler thread")?; */
    }

    let mut anchor = "3juj4".to_string();
    loop {
        info!(target: "repo_indexer", "anchor {}", anchor);

        let mut res = state
            .db
            .query(format!(
                "SELECT id,in,out FROM follow:{}.. LIMIT 500000;",
                anchor
            ))
            .await?;
        let likes_res: Vec<BskyFollowRes> = res.take(0)?;

        if likes_res.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(10000)).await;
            continue;
        }

        anchor = format!("{}", likes_res.last().unwrap().id.key());

        let mut dids: BTreeSet<String> = BTreeSet::new();

        for like in likes_res {
            for record_id in vec![like.from, like.to] {
                let did = unsafe_user_key_to_did(&format!("{}", record_id.key()));
                if !processed_dids.contains(&did) {
                    dids.insert(did.clone());
                    processed_dids.insert(did);
                }
            }
        }

        for did in dids {
            tx.send(did)
                .await
                .context("Failed to send message to handler thread")?;
        }

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

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
    rx: async_channel::Receiver<String>,
    db: Surreal<Any>,
    http_client: Client,
}

async fn task_handler(state: Arc<SharedState>, task_id: u64) -> anyhow::Result<()> {
    tokio::time::sleep(std::time::Duration::from_millis(task_id * 42)).await;
    // loop infinitely, handling repo index tasks
    loop {
        // get the next repo to be indexed
        let did = {
            let x = state.rx.recv().await;
            x.unwrap()
        };

        let res = index_repo(&state, &did).await;
        if let Err(e) = res {
            let e_str = format!("{}", e);
            if e_str == "Failed to parse CAR file: early eof" {
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
                        error: Some(e_str),
                    })
                    .await?;
            } else {
                warn!(target: "indexer", "Failed to index repo {}: {}", did, e);
            }
            /* match e.try_into() {
                iroh_car::Error::Parsing(e) => {}
                _ => {}
            } */
        }
    }
}

/// Database struct for a repo indexing timestamp
#[derive(Debug, Serialize, Deserialize)]
pub struct LastIndexedTimestamp {
    pub time_us: u64,
    pub time_dt: surrealdb::Datetime,
    pub error: Option<String>,
}
