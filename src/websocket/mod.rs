use anyhow::Context;
use fastwebsockets::{OpCode, WebSocket};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use sqlx::PgPool;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use surrealdb::{engine::any::Any, Surreal};
use tokio::time::sleep;
use tokio_rustls::{
    rustls::{
        pki_types::{pem::PemObject, CertificateDer},
        ClientConfig, RootCertStore,
    },
    TlsConnector,
};
use tracing::{debug, info, trace, warn};

mod conn;
pub mod events;
mod handler;

/// Shared state for the websocket module
#[derive(Debug)]
struct SharedState {
    host: String,
    db: Surreal<Any>,
    database: PgPool,
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
    database: PgPool,
) -> anyhow::Result<()> {
    // prepare tls store
    let cloned_certificate_path = certificate.clone();
    debug!(target: "indexer", "Creating tls store for certificate: {}", cloned_certificate_path);
    let mut tls_store = RootCertStore::empty();
    let tls_cert = CertificateDer::from_pem_file(certificate).with_context(|| {
        format!(
            "Unable to parse certificate from: {}",
            cloned_certificate_path
        )
    })?;
    tls_store.add(tls_cert).with_context(|| {
        format!(
            "Unable to add certificate to tls store: {}",
            cloned_certificate_path
        )
    })?;
    let tls_config = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(Arc::new(tls_store))
            .with_no_client_auth(),
    );
    let connector = TlsConnector::from(tls_config.clone());

    // create a shared state
    info!(target: "indexer", "Entering websocket loop");
    let state = Arc::new(SharedState {
        host: host.clone(),
        db,
        cursor: AtomicU64::new(cursor),
        database,
    });

    // loop infinitely, ensuring connection aborts are handled
    loop {
        // get current cursor
        let cursor = {
            let c = state.cursor.load(Ordering::Relaxed);
            if c == 0 {
                None
            } else {
                Some(c)
            }
        };

        // create websocket connection
        info!(target: "indexer", "Establishing new connection to: {}", host);
        let ws = conn::connect_tls(&host, &connector, cursor).await;
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
            let cursor = state.cursor.fetch_sub(REWIND_TIME, Ordering::Relaxed);
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

                let res = handler::handle_message(state, text, update_cursor).await;

                if res.is_err() {
                    warn!("error while handling {}", res.unwrap_err());
                }
            }
        };
    }
}
