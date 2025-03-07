use anyhow::Context;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use surrealdb::{engine::any::Any, Datetime, RecordId, Surreal};
use tracing::debug;

/// Initialize the database with the necessary definitions
pub async fn init(db: PgPool) -> anyhow::Result<()> {
    // TODO Add all types
    let schema = r#"
"#;

    Ok(())
}
