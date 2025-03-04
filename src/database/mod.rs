use std::{sync::LazyLock, time::Duration};

use anyhow::{Context, Result};
use definitions::{JetstreamCursor, Record};
use surrealdb::{
    engine::{
        any::Any,
        remote::ws::{Client, Ws},
    },
    opt::{
        auth::{Credentials, Root},
        Config,
    },
    RecordId, Surreal,
};
use tracing::{debug, info};

use crate::config::ARGS;

pub mod definitions;
pub mod handlers;
pub mod repo_indexer;
mod utils;

static DB: LazyLock<Surreal<Client>> = LazyLock::new(Surreal::init);

/// Connect to the database
pub async fn connect(
    db_endpoint: &str,
    username: &str,
    password: &str,
) -> anyhow::Result<Surreal<Any>> {
    // connect to the database
    info!(target: "indexer", "Connecting to the database at {}", db_endpoint);
    // let db = Surreal::new::<_>(db_endpoint).await?;
    let db = surrealdb::engine::any::connect(db_endpoint)
        .with_capacity(ARGS.surrealdb_capacity)
        .await?;
    db.signin(Root {
        username: "root",
        password: "root",
    })
    .await?;

    //     let config = Config::default().query_timeout(Duration::from_millis(1500));
    // let dbb = DB.connect::<Ws>("127.0.0.1:8000", Op)

    // sign in to the server

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
