use super::connect;
use crate::config::ARGS;
use futures::{stream::FuturesUnordered, StreamExt};
use index_repo::PipelineItem;
use opentelemetry::{global, KeyValue};
use pumps::Concurrency;
use repo_stream::RepoStream;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{ops::Rem, sync::LazyLock};
use surrealdb::{engine::any::Any, Surreal};
use tracing::warn;

mod index_repo;
mod repo_stream;

/// Database struct for a repo indexing timestamp
#[derive(Debug, Serialize, Deserialize)]
pub struct LastIndexedTimestamp {
    pub time_us: u64,
    pub time_dt: surrealdb::Datetime,
    pub error: Option<String>,
}

/// An ID that was used before the earliest data we are interested in
const OLDEST_USEFUL_ANCHOR: &str = "3juj4";

// Make this less hacky
macro_rules! pump_stage {
    ($metric:ident, $perfmetric:ident, $stage:literal, $next:literal, $function:ident) => {
        |x| async {
            tokio::task::spawn(async move {
                eprintln!("starting {}", $stage);

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
                tokio::time::sleep(::tokio::time::Duration::from_secs(1)).await;
                let before = std::time::Instant::now();
                let result = tokio::time::timeout(
                    tokio::time::Duration::from_secs(ARGS.pipeline_stage_timeout),
                    x.$function(),
                )
                .await;
                let duration = before.elapsed();
                eprintln!(
                    "pre         finished {} in {:02}",
                    $stage,
                    duration.as_millis() as f64 / 1000.0
                );
                let Ok(result) = result else {
                    panic!("Timeout in {}", $stage);
                };

                // $perfmetric.record(
                //     duration.as_millis() as u64,
                //     &[KeyValue::new("stage", $stage)],
                // );
                $metric.add(
                    -1,
                    &[
                        KeyValue::new("stage", $stage),
                        KeyValue::new("state", "active"),
                    ],
                );

                let result = match result {
                    Err(error) => {
                        eprintln!(
                            "failed {} in {:02}",
                            $stage,
                            duration.as_millis() as f64 / 1000.0
                        );

                        // error!(target: "indexer", "Failed to index repo: {}", error);
                        return None;
                    }
                    Ok(result) => result,
                };

                if $next != "done" {
                    $metric.add(
                        1,
                        &[
                            KeyValue::new("stage", $next),
                            KeyValue::new("state", "queued"),
                        ],
                    );
                }
                eprintln!(
                    "finished {} in {:02}",
                    $stage,
                    duration.as_millis() as f64 / 1000.0
                );
                return Some(result);
            })
            .await
            .expect("Failed to spawn task in a pump stage")
        }
    };
}

const TRACKER: LazyLock<opentelemetry::metrics::UpDownCounter<i64>> = LazyLock::new(|| {
    global::meter("indexer")
        .i64_up_down_counter("indexer.pipeline.location")
        .with_description("Track the number of tasks in the pipeline")
        .with_unit("repo")
        .build()
});

pub async fn start_full_repo_indexer(db: Surreal<Any>) -> anyhow::Result<()> {
    let http_client = Client::new();

    let meter = global::meter("indexer");
    let repos_indexed = meter
        .u64_counter("indexer.repos.indexed")
        .with_description("Total number of indexed repos")
        .with_unit("repo")
        .build();
    let job_duration = meter
        .u64_histogram("indexer.pipeline.duration")
        .with_unit("ms")
        .with_description("Pipeline job duration")
        .with_boundaries(
            vec![1, 3, 10, 31, 100, 316, 1000, 3160, 10000]
                .iter()
                .map(|x| *x as f64 + 1000.0)
                .collect::<Vec<f64>>(),
        )
        .build();

    let mut res = db
        .query("SELECT count() as c FROM latest_backfill WHERE at != NONE GROUP ALL;")
        .await
        .unwrap();
    let count = res.take::<Option<i64>>((0, "c")).unwrap().unwrap_or(0);
    if count == 0 {
        warn!("Started with 0 repos, this might be a bug");
    }
    repos_indexed.add(count as u64, &[]);

    let buffer_size = ARGS.pipeline_buffer_size;
    let download_concurrency_multiplier = ARGS.pipeline_download_concurrency_multiplier;
    let concurrent_elements = ARGS.pipeline_concurrent_elements;

    let databases = ARGS
        .db
        .iter()
        .map(|x| async { connect(x, &ARGS.username, &ARGS.password).await.unwrap() })
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<_>>()
        .await;
    let repo_stream = RepoStream::new(OLDEST_USEFUL_ANCHOR.to_string(), db.clone());
    let dids = repo_stream.enumerate().map(move |(id, x)| {
        (
            x.to_string(),
            databases.get(id.rem(databases.len())).unwrap().clone(),
            http_client.clone(),
        )
    });

    let (mut output_receiver, _join_handle) = pumps::Pipeline::from_stream(dids)
        .map(
            |(did, db, http_client)| async {
                let item = PipelineItem::new(db, http_client, did);

                TRACKER.add(
                    1,
                    &[
                        KeyValue::new("stage", "get_service"),
                        KeyValue::new("state", "queued"),
                    ],
                );
                item
            },
            Concurrency::concurrent_unordered(concurrent_elements),
        )
        .backpressure(buffer_size)
        .filter_map(
            pump_stage!(
                TRACKER,
                job_duration,
                "get_service",
                "download_repo",
                get_service
            ),
            Concurrency::concurrent_unordered(concurrent_elements),
        )
        .backpressure(buffer_size)
        .filter_map(
            pump_stage!(
                TRACKER,
                job_duration,
                "download_repo",
                "process_repo",
                download_repo
            ),
            Concurrency::concurrent_unordered(
                concurrent_elements * download_concurrency_multiplier,
            ),
        )
        .backpressure(buffer_size)
        .filter_map(
            pump_stage!(
                TRACKER,
                job_duration,
                "process_repo",
                "apply_updates",
                process_repo
            ),
            Concurrency::concurrent_unordered(concurrent_elements),
        )
        .backpressure(buffer_size)
        .filter_map(
            pump_stage!(
                TRACKER,
                job_duration,
                "apply_updates",
                "print_report",
                apply_updates
            ),
            Concurrency::concurrent_unordered(concurrent_elements),
        )
        .backpressure(buffer_size)
        .filter_map(
            pump_stage!(TRACKER, job_duration, "print_report", "done", print_report),
            Concurrency::concurrent_unordered(concurrent_elements),
        )
        .backpressure(buffer_size)
        // .map(download_heavy_resource, Concurrency::serial())
        // .filter_map(run_algorithm, Concurrency::concurrent_unordered(concurrent_elements))
        // .map(save_to_db, Concurrency::concurrent_unordered(100))
        .build();
    // join_handle.await;
    let mut elements = 0;
    loop {
        let Some(_result) = output_receiver.recv().await else {
            panic!("Done, this should not happen");
        };
        elements += 1;
        repos_indexed.add(1, &[]);
        eprintln!("Finished: {}", elements);
    }

    Ok(())
}
