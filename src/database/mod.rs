use std::time::Duration;

use anyhow::Result;
use definitions::JetstreamCursor;
use sqlx::{postgres::PgPoolOptions, PgPool};

use crate::config::ARGS;

pub mod big_update;
pub mod definitions;
pub mod handlers;
pub mod repo_indexer;
mod utils;

/// Connect to the database
pub async fn connect() -> anyhow::Result<PgPool> {
    // connect to the database
    let database = PgPoolOptions::new()
        .max_connections(ARGS.db_pool_size)
        .acquire_slow_threshold(Duration::from_secs(20))
        .connect(&ARGS.db)
        .await?;

    sqlx::migrate!("./migrations").run(&database).await?;

    Ok(database)
}

// /// Connect to the database
// pub async fn connect_surreal(db_endpoint: &str) -> anyhow::Result<Surreal<Any>> {
//     // connect to the database
//     info!(target: "indexer", "Connecting to the database at {}", db_endpoint);
//     let db = surrealdb::engine::any::connect(db_endpoint)
//         .with_capacity(ARGS.surrealdb_capacity)
//         .await?;
//     db.signin(Root {
//         username: &ARGS.username,
//         password: &ARGS.password,
//     })
//     .await?;

//     definitions::init(&db)
//         .await
//         .context("Failed to initialize database schema")?;

//     Ok(db)
// }

/// Fetch the current cursor from the database
pub async fn fetch_cursor(
    db: impl sqlx::PgExecutor<'_>,
    host: &str,
) -> Result<Option<JetstreamCursor>> {
    let res = sqlx::query_as!(
        JetstreamCursor,
        "SELECT * FROM jetstream_cursor WHERE host = $1",
        host
    )
    .fetch_optional(db)
    .await?;

    Ok(res)
}

/// Write the cursor to the database
pub async fn write_cursor(db: impl sqlx::PgExecutor<'_>, cursor: JetstreamCursor) -> Result<()> {
    // let _: Option<Record> = db
    //     .upsert(("cursor", host))
    //     .content(JetstreamCursor {
    //         time_us: (cursor - 10_000_000),
    //     })
    //     .await?;
    sqlx::query!(
        "INSERT INTO jetstream_cursor (host, time_us) VALUES ($1, $2) ON CONFLICT (host) DO UPDATE SET time_us = $2",
        &cursor.host,
        &cursor.time_us
    ).execute(db).await?;

    Ok(())
}
