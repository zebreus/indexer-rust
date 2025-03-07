use crate::config::ARGS;
use anyhow::{Context, Result};
use definitions::{JetstreamCursor, Record};
use sqlx::{postgres::PgPoolOptions, PgPool};
use surrealdb::{engine::any::Any, opt::auth::Root, RecordId, Surreal};
use tracing::info;

pub mod big_update;
pub mod definitions;
pub mod handlers;
pub mod repo_indexer;
mod utils;

/// Connect to the database
pub async fn connect() -> anyhow::Result<PgPool> {
    // connect to the database
    let database = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://user-name:strong-password@localhost/files")
        .await?;

    Ok(database)
}

/// Connect to the database
pub async fn connect_surreal(db_endpoint: &str) -> anyhow::Result<Surreal<Any>> {
    // connect to the database
    info!(target: "indexer", "Connecting to the database at {}", db_endpoint);
    let db = surrealdb::engine::any::connect(db_endpoint)
        .with_capacity(ARGS.surrealdb_capacity)
        .await?;
    db.signin(Root {
        username: &ARGS.username,
        password: &ARGS.password,
    })
    .await?;

    definitions::init(&db)
        .await
        .context("Failed to initialize database schema")?;

    Ok(db)
}

/// Fetch the current cursor from the database
pub async fn fetch_cursor(db: &Surreal<Any>, host: &str) -> Result<Option<JetstreamCursor>> {
    let res: Option<JetstreamCursor> = db.select(("cursor", host)).await?;

    Ok(res)
}

/// Write the cursor to the database
pub async fn write_cursor(db: &Surreal<Any>, host: &str, cursor: u64) -> Result<()> {
    let _: Option<Record> = db
        .upsert(("cursor", host))
        .content(JetstreamCursor {
            time_us: (cursor - 10_000_000),
        })
        .await?;

    Ok(())
}

/// Delete a record from the database
async fn delete_record(db: &Surreal<Any>, table: &str, key: &str) -> anyhow::Result<()> {
    let _: Option<Record> = db.delete(RecordId::from_table_key(table, key)).await?;

    Ok(())
}
