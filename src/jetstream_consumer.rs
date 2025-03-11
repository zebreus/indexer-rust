use crate::{database, websocket};
use anyhow::Context;
use futures::{stream::FuturesUnordered, StreamExt};
use sqlx::PgPool;
use tracing::error;

const JETSTREAM_HOSTS: [&str; 5] = [
    "jetstream1.us-west.bsky.network",
    "jetstream2.us-east.bsky.network",
    "test-jetstream.skyfeed.moe",
    "jetstream2.us-west.bsky.network",
    "jetstream1.us-east.bsky.network",
];

pub async fn attach_jetstream(database: PgPool) -> anyhow::Result<()> {
    let mut jetstream_tasks = JETSTREAM_HOSTS
        .iter()
        .map(|host| {
            tokio::task::spawn(start_jetstream_consumer(database.clone(), host.to_string()))
        })
        .collect::<FuturesUnordered<_>>();

    loop {
        let result = jetstream_tasks.next().await;
        let Some(Ok(Ok(_))) = result else {
            error!("Jetstream consumer task failed");
            break;
        };
    }

    error!("All jetstream consumer task failed");

    Ok(())
}

async fn start_jetstream_consumer(database: PgPool, host: String) -> anyhow::Result<()> {
    // fetch initial cursor
    let cursor = database::fetch_cursor(&database, &host)
        .await
        .context("Failed to fetch cursor from database")?
        .map_or(0, |e| e.time_us);

    // enter websocket event loop
    websocket::start(host, cursor, database)
        .await
        .context("WebSocket event loop failed")?;

    Ok(())
}
