use anyhow::Context;
use futures::{stream::FuturesUnordered, StreamExt};
use surrealdb::{engine::any::Any, Surreal};

use crate::{database, websocket};

const JETSTREAM_HOSTS: [&str; 5] = [
    "jetstream1.us-west.bsky.network",
    "jetstream2.us-east.bsky.network",
    "test-jetstream.skyfeed.moe",
    "jetstream2.us-west.bsky.network",
    "jetstream1.us-east.bsky.network",
];

pub async fn attach_jetstream(db: Surreal<Any>, certificate: String) -> anyhow::Result<()> {
    let mut jetstream_tasks = JETSTREAM_HOSTS
        .iter()
        .map(|host| start_jetstream_consumer(db.clone(), host.to_string(), certificate.clone()))
        .collect::<FuturesUnordered<_>>();

    loop {
        let result = jetstream_tasks.next().await;
        let Some(result) = result else {
            break;
        };
        result?;
    }

    Ok(())
}

async fn start_jetstream_consumer(
    db: Surreal<Any>,
    host: String,
    certificate: String,
) -> anyhow::Result<()> {
    // fetch initial cursor
    let cursor = database::fetch_cursor(&db, &host)
        .await
        .context("Failed to fetch cursor from database")?
        .map_or(0, |e| e.time_us);

    // enter websocket event loop
    websocket::start(host, certificate, cursor, db)
        .await
        .context("WebSocket event loop failed")?;

    Ok(())
}
