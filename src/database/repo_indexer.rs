use futures::StreamExt;
use index_repo::PipelineItem;
use repo_stream::RepoStream;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use surrealdb::{engine::any::Any, Surreal};
use tracing::{error, info};

mod index_repo;
mod repo_stream;

#[derive(Deserialize)]
struct BskyFollowRes {
    #[serde(rename = "in")]
    pub from: surrealdb::RecordId,
    #[serde(rename = "out")]
    pub to: surrealdb::RecordId,
    pub id: surrealdb::RecordId,
}

/// Database struct for a repo indexing timestamp
#[derive(Debug, Serialize, Deserialize)]
pub struct LastIndexedTimestamp {
    pub time_us: u64,
    pub time_dt: surrealdb::Datetime,
    pub error: Option<String>,
}

/// An ID that was used before the earliest data we are interested in
const OLDEST_USEFUL_ANCHOR: &str = "3juj4";
/// The size of the buffer between each pipeline stage in elements
const BUFFER_SIZE: usize = 200;

pub async fn start_full_repo_indexer(db: Surreal<Any>, max_tasks: usize) -> anyhow::Result<()> {
    let http_client = Client::new();

    info!(target: "indexer", "Spinning up {} handler tasks", max_tasks);

    RepoStream::new(OLDEST_USEFUL_ANCHOR.to_string(), &db)
        .map(|did| async { did })
        .buffer_unordered(BUFFER_SIZE)
        .map(|did| {
            let db = &db;
            let http_client = &http_client;
            let item = PipelineItem::new(db, http_client, did);
            item
        })
        .map(|item| async { item.check_indexed().await })
        .buffer_unordered(BUFFER_SIZE)
        .filter_map(|result| async {
            if let Err(error) = &result {
                error!(target: "indexer", "Failed to index repo: {}", error);
            }
            result.ok()
        })
        .map(|item| async { item.get_service().await })
        .buffer_unordered(BUFFER_SIZE)
        .filter_map(|result| async {
            if let Err(error) = &result {
                error!(target: "indexer", "Failed to index repo: {}", error);
            }
            result.ok()
        })
        .map(|item| async { item.download_repo().await })
        .buffer_unordered(BUFFER_SIZE)
        .filter_map(|result| async {
            if let Err(error) = &result {
                error!(target: "indexer", "Failed to index repo: {}", error);
            }
            result.ok()
        })
        .map(|item| async { item.deserialize_repo().await })
        .buffer_unordered(BUFFER_SIZE)
        .filter_map(|result| async {
            if let Err(error) = &result {
                error!(target: "indexer", "Failed to index repo: {}", error);
            }
            result.ok()
        })
        .map(|item| async { item.files_to_updates().await })
        .buffer_unordered(BUFFER_SIZE)
        .filter_map(|result| async {
            if let Err(error) = &result {
                error!(target: "indexer", "Failed to index repo: {}", error);
            }
            result.ok()
        })
        .map(|item| async { item.apply_updates().await })
        .buffer_unordered(BUFFER_SIZE)
        .filter_map(|result| async {
            if let Err(error) = &result {
                error!(target: "indexer", "Failed to index repo: {}", error);
            }
            result.ok()
        })
        .for_each(|x| async {
            x.print_report().await;
        })
        .await;

    panic!("Done, this should not happen");

    // .map(|did| {
    //     // let state = state.clone();
    //     let db = &db;
    //     let client = &client;
    //     async move {
    //         let result = index_repo(&db, &client, &did).await;
    //         if let Err(error) = result {
    //             warn!(target: "indexer", "Failed to index repo {}: {}", did, error);

    //             let error_message = format!("{}", error);
    //             if format!("{}", error) == "Failed to parse CAR file: early eof" {
    //                 // TODO: Document what this case does

    //                 let did_key = crate::database::utils::did_to_key(did.as_str()).unwrap();
    //                 let timestamp_us = std::time::SystemTime::now()
    //                     .duration_since(std::time::UNIX_EPOCH)
    //                     .unwrap()
    //                     .as_micros();
    //                 let _: Option<super::definitions::Record> = db
    //                     .upsert(("li_did", did_key))
    //                     .content(LastIndexedTimestamp {
    //                         time_us: timestamp_us as u64,
    //                         time_dt: chrono::Utc::now().into(),
    //                         error: Some(error_message),
    //                     })
    //                     .await
    //                     .unwrap();
    //             }
    //         }
    //     }
    // })
    // .buffer_unordered(200)
    // .for_each(|x| async {
    //     println!("finished stream");
    // })
    // .await;

    // for thread_id in 0..max_tasks {
    //     let state = state.clone();
    //     tokio::spawn(async move {
    //         let result = repo_fetcher_task(state).await;
    //         let error = result
    //             .and::<()>(Err(anyhow!("Handler thread should never exit")))
    //             .unwrap_err();
    //         error!(target: "indexer", "Handler thread {} failed: {:?}", thread_id, error);
    //     });
    // }

    // repo_discovery_task(state, tx).await.unwrap();
}
