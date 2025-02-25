use futures::StreamExt;
use index_repo::PipelineItem;
use opentelemetry::global;
use repo_stream::RepoStream;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use surrealdb::{engine::any::Any, Surreal};
use tracing::{error, warn};

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

pub async fn start_full_repo_indexer(db: &Surreal<Any>) -> anyhow::Result<()> {
    let http_client = Client::new();

    let meter = global::meter("indexer");
    let repos_indexed = meter
        .u64_counter("indexer.repos.indexed")
        .with_description("Total number of indexed repos")
        .with_unit("repo")
        .build();

    let mut res = db
        .query("SELECT count() as c FROM li_did GROUP ALL;")
        .await
        .unwrap();
    let count = res.take::<Option<i64>>((0, "c")).unwrap().unwrap_or(0);
    if count == 0 {
        warn!("Started with 0 repos, this might be a bug");
    }
    repos_indexed.add(count as u64, &[]);

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
            repos_indexed.add(1, &[]);
        })
        .await;

    panic!("Done, this should not happen");
}
