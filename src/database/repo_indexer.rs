use futures::StreamExt;
use index_repo::PipelineItem;
use opentelemetry::{global, KeyValue};
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
const BUFFER_SIZE: usize = 100;
/// Buffer size multiplier for the download stage
const DOWNLOAD_BUFFER_SIZE: usize = 1;

// Make this less hacky
macro_rules! stage {
    ($metric:ident, $stage:literal, $next:literal, $item:ident -> $content:expr) => {
        |$item| async {
            // TODO: Dont create new keyvalues every time
            $metric.add(
                -1,
                &[
                    KeyValue::new("stage", $stage),
                    KeyValue::new("state", "queued"),
                ],
            );
            $metric.add(
                1,
                &[
                    KeyValue::new("stage", $stage),
                    KeyValue::new("state", "active"),
                ],
            );

            let result = $content;

            $metric.add(
                -1,
                &[
                    KeyValue::new("stage", $stage),
                    KeyValue::new("state", "active"),
                ],
            );
            $metric.add(
                1,
                &[
                    KeyValue::new("stage", $next),
                    KeyValue::new("state", "queued"),
                ],
            );

            result
        }
    };
}

// Make this less hacky
macro_rules! filter_result {
    ($metric:ident, $stage:literal) => {|result| async {
        if let Err(error) = &result {
            error!(target: "indexer", "Failed to index repo: {}", error);
            $metric.add(
                -1,
                &[
                    KeyValue::new("stage", $stage),
                    KeyValue::new("state", "queued"),
                ],
            );
            return None;
        }
        result.ok()
    }};
}

// async fn filter_result<T>(result: anyhow::Result<T>) -> Option<T> {
//     if let Err(error) = &result {
//         error!(target: "indexer", "Failed to index repo: {}", error);
//     }
//     result.ok()
// }

pub async fn start_full_repo_indexer(db: Surreal<Any>) -> anyhow::Result<()> {
    let http_client = Client::new();

    let meter = global::meter("indexer");
    let repos_indexed = meter
        .u64_counter("indexer.repos.indexed")
        .with_description("Total number of indexed repos")
        .with_unit("repo")
        .build();
    let tracker = meter
        .i64_up_down_counter("indexer.pipeline.location")
        .with_description("Track the number of tasks in the pipeline")
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
        .map(|did| async {
            let db = &db;
            let http_client = &http_client;
            let item = PipelineItem::new(db, http_client, did);

            tracker.add(
                1,
                &[
                    KeyValue::new("stage", "check_indexed"),
                    KeyValue::new("state", "queued"),
                ],
            );
            item
        })
        .buffer_unordered(BUFFER_SIZE)
        .map(stage!(tracker, "check_indexed", "get_service", item ->
            item.check_indexed().await
        ))
        .buffer_unordered(BUFFER_SIZE)
        .filter_map(filter_result!(tracker, "get_service"))
        .map(stage!(tracker, "get_service", "download_repo", item ->
            item.get_service().await
        ))
        .buffer_unordered(BUFFER_SIZE)
        .filter_map(filter_result!(tracker, "download_repo"))
        .map(stage!(tracker, "download_repo", "deserialize_repo", item ->
            item.download_repo().await
        ))
        .buffer_unordered(BUFFER_SIZE * DOWNLOAD_BUFFER_SIZE)
        .filter_map(filter_result!(tracker, "deserialize_repo"))
        .map(
            stage!(tracker, "deserialize_repo", "files_to_updates", item ->
                item.deserialize_repo().await
            ),
        )
        .buffer_unordered(BUFFER_SIZE)
        .filter_map(filter_result!(tracker, "files_to_updates"))
        .map(stage!(tracker, "files_to_updates", "apply_updates", item ->
            item.files_to_updates().await
        ))
        .buffer_unordered(BUFFER_SIZE)
        .filter_map(filter_result!(tracker, "apply_updates"))
        .map(stage!(tracker, "apply_updates", "print_report", item ->
            {
                // println!("Items: {:?}", item.state.updates.len());
                item.apply_updates().await}
        ))
        .buffer_unordered(BUFFER_SIZE)
        .filter_map(filter_result!(tracker, "print_report"))
        .for_each(|x| async {
            tracker.add(
                -1,
                &[
                    KeyValue::new("stage", "print_report"),
                    KeyValue::new("state", "queued"),
                ],
            );
            tracker.add(
                1,
                &[
                    KeyValue::new("stage", "print_report"),
                    KeyValue::new("state", "active"),
                ],
            );
            x.print_report().await;
            tracker.add(
                -1,
                &[
                    KeyValue::new("stage", "print_report"),
                    KeyValue::new("state", "active"),
                ],
            );
            repos_indexed.add(1, &[]);
        })
        .await;

    // panic!("Done, this should not happen");
    Ok(())
}
