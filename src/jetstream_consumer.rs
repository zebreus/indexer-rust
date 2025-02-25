use anyhow::Context;
use surrealdb::{engine::any::Any, Surreal};
use tokio::runtime::Builder;

use crate::{database, websocket};

pub async fn attach_jetstream(db: Surreal<Any>, certificate: String) -> anyhow::Result<()> {
    let jetstream_hosts = vec![
        "jetstream1.us-west.bsky.network",
        "jetstream2.us-east.bsky.network",
        "test-jetstream.skyfeed.moe",
        "jetstream2.us-west.bsky.network",
        "jetstream1.us-east.bsky.network",
    ];

    for host in jetstream_hosts {
        let db_clone = db.clone();
        let certificate = certificate.clone();
        let (name, _) = host.split_at(18);
        std::thread::Builder::new()
            .name(format!("{}", name))
            .spawn(move || {
                Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                    .unwrap()
                    .block_on(async {
                        start_jetstream_consumer(db_clone, host.to_string(), certificate)
                            .await
                            .context("jetstream consumer failed")
                            .unwrap();
                    });
            })
            .context("Failed to spawn jetstream consumer thread")?;
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
