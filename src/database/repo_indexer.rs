use crate::config::ARGS;
use futures::StreamExt;
use index_repo::DownloadService;
use pipeline::{create_stage, next_stage};
use repo_stream::RepoStream;
use reqwest::Client;
use sqlx::PgPool;
use tracing::error;

mod index_repo;
mod pipeline;
mod repo_stream;

macro_rules! unordered {
    ($concurrency:expr) => {
        pumps::Concurrency::concurrent_unordered($concurrency)
    };
}

pub async fn start_full_repo_indexer(database: PgPool) -> anyhow::Result<()> {
    let http_client = Client::new();

    let buffer_size = ARGS.pipeline_buffer_size;
    let download_concurrency_multiplier = ARGS.pipeline_download_concurrency_multiplier;
    let concurrent_elements = ARGS.pipeline_concurrent_elements;
    let download_concurrent_elements = concurrent_elements * download_concurrency_multiplier;

    // Create a stream of dids + captured database and http client
    let dids = RepoStream::new(database.clone())
        .enumerate()
        .map(move |(id, did)| (did, database.clone(), http_client.clone()));

    // Create the processing pipeline
    let (mut output_receiver, _join_handle) = pumps::Pipeline::from_stream(dids)
        .filter_map(
            create_stage(|(did, database, http_client)| {
                DownloadService::new(database, http_client, did)
            }),
            unordered!(concurrent_elements),
        )
        .backpressure(buffer_size)
        .filter_map(next_stage(), unordered!(concurrent_elements))
        .backpressure(buffer_size)
        .filter_map(next_stage(), unordered!(download_concurrent_elements))
        .backpressure(buffer_size)
        .filter_map(next_stage(), unordered!(concurrent_elements))
        .backpressure(buffer_size)
        .filter_map(next_stage(), unordered!(concurrent_elements))
        .backpressure(buffer_size)
        .build();

    // Process items
    loop {
        let Some(_result) = output_receiver.recv().await else {
            error!("Backfill pipeline ran out of items. This should never happen.");
            panic!("Backfill pipeline ran out of items. This should never happen.");
        };
    }
}
