use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::Context;
use fastwebsockets::{OpCode, WebSocket};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use log::{info, trace, warn};
use surrealdb::{engine::any::Any, Surreal};
use tokio::time::sleep;

mod conn;
pub mod events;
mod handler;

/// Shared state for the websocket module
#[derive(Debug)]
struct SharedState {
    host: String,
    db: Surreal<Any>,
    cursor: AtomicU64,
}

impl SharedState {
    /// Update the cursor
    pub fn update_cursor(&self, cursor: u64) {
        self.cursor.store(cursor, Ordering::Relaxed);
    }
}

/// Subscribe to a websocket server
pub async fn start(
    host: String,
    certificate: String,
    cursor: u64,
    db: Surreal<Any>,
) -> anyhow::Result<()> {
    // create a shared state
    info!(target: "indexer", "Entering websocket loop");
    let state = Arc::new(SharedState {
        host: host.clone(),
        db,
        cursor: AtomicU64::new(cursor),
    });

    // loop infinitely, ensuring connection aborts are handled
    loop {
        // get current cursor
        let cursor = {
            let c = (&state).cursor.load(Ordering::Relaxed) as u64;
            if c == 0 {
                None
            } else {
                Some(c)
            }
        };

        // create websocket connection
        info!(target: "indexer", "Establishing new connection to: {}", host);
        let ws = conn::connect_tls(&host, &certificate, cursor).await;
        if let Err(e) = ws {
            warn!(target: "indexer", "Unable to open websocket connection to {}: {:?}", host, e);
            sleep(Duration::from_secs(5)).await;
            continue;
        }
        let ws = ws.unwrap();

        // handle the websocket connection
        info!(target: "indexer", "Handling websocket connection starting at cursor: {:?}", cursor);
        let res = manage_ws(&state, ws).await;
        if let Err(e) = res {
            warn!(target: "indexer", "Websocket connection failed: {:?}", e);
        }

        // rewind cursor by 10 seconds
        {
            const REWIND_TIME: u64 = 10_000_000; // 10 seconds in microseconds
            let cursor = (&state).cursor.fetch_sub(REWIND_TIME, Ordering::Relaxed);
            info!(target: "indexer", "Rewinding cursor by 10 seconds: {} -> {}", cursor, cursor - REWIND_TIME);
        }

        // let the server breathe
        sleep(Duration::from_millis(200)).await;
    }
}

async fn manage_ws(
    state: &SharedState,
    mut ws: WebSocket<TokioIo<Upgraded>>,
) -> anyhow::Result<()> {
    let mut time = Instant::now();
    loop {
        // try to read a message
        let msg = ws
            .read_frame()
            .await
            .context("Failed to read frame from websocket")?;

        // check if cursor needs an update
        let update_cursor = if time.elapsed().as_secs() >= 60 {
            time = Instant::now();
            true
        } else {
            false
        };

        // handle message
        match msg.opcode {
            // spec states only text frames are allowed
            OpCode::Continuation | OpCode::Binary | OpCode::Ping | OpCode::Pong => {
                warn!(target: "indexer", "Unexpected opcode received: {:?}", msg.opcode);
            }
            // can be emitted by the server
            OpCode::Close => {
                anyhow::bail!("Unexpected connection close received: {:?}", msg.payload);
            }
            // handle text message
            OpCode::Text => {
                trace!(target: "indexer", "Received text message: {}", msg.payload.len());
                let text = String::from_utf8(msg.payload.to_vec())
                    .context("Failed to decode text message")?;

                let res = handler::handle_message(&state, text, update_cursor).await;

                if res.is_err() {
                    warn!("error while handling {}", res.unwrap_err());
                }
            }
        };
    }
}
