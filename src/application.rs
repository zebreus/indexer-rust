use std::sync::{LazyLock, RwLock};

use anyhow::Context;
use events::Kind;
use fastwebsockets::{Frame, OpCode, WebSocket};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use log::{error, info, warn};
use tokio::time::sleep;

mod ws;
mod events;

/// List of wanted collections (seems to still let through some requests, weird)
const NSIDS: [&str; 15] = [
    "app.bsky.actor.profile",
    "app.bsky.feed.generator",
    "app.bsky.feed.like",
    "app.bsky.feed.post",
    "app.bsky.feed.postgate",
    "app.bsky.feed.repost",
    "app.bsky.feed.threadgate",
    "app.bsky.graph.block",
    "app.bsky.graph.follow",
    "app.bsky.graph.list",
    "app.bsky.graph.listblock",
    "app.bsky.graph.listitem",
    "app.bsky.graph.starterpack",
    "app.bsky.labeler.service",
    "chat.bsky.actor.declaration"
];

static CURSOR: LazyLock<RwLock<u64>> = LazyLock::new(|| RwLock::new(0));

///
/// Main function for the application
///
/// # Arguments
///
/// * `host` - The host to connect to
/// * `cert` - The certificate to use for the connection
/// * `cursor` - The optional cursor to start playback from
///
/// # Returns
///
/// * `Result<(), anyhow::Error>` - The result of the operation
///
pub async fn launch_client(host: &String, cert: &String, initial_cursor: u64) ->
    Result<(), anyhow::Error> {

    // update current cursor initially
    {
        let mut cursor = CURSOR.write().unwrap();
        *cursor = initial_cursor;
    }

    // loop infinitely, ensuring connection aborts are handled
    loop {

        // get current cursor
        let cursor = {
            let cursor = CURSOR.read().unwrap();
            *cursor
        };

        // create a new connection
        let ws =
           ws::connect(host, cert, cursor, NSIDS.to_vec())
            .await.context("failed to establish connection to jetstream")?;
        info!(target: "jetstream", "established new connection to jetstream server");

        let res = handle_ws(ws).await;
        if res.is_err() {
            error!(target: "jetstream", "error handling websocket: {:?}", res.err().unwrap());
        }

        // rewind cursor by 2 seconds
        {
            let mut cursor = CURSOR.write().unwrap();
            *cursor -= 2;
            info!(target: "jetstream", "rewinding cursor by 2 seconds: {} -> {}", cursor, *cursor);
        }

        // give the server 200 ms to recover
        sleep(std::time::Duration::from_millis(200)).await;

    }
}

///
/// Handle the websocket connection
///
/// # Arguments
///
/// * `ws` - The websocket connection
///
/// # Returns
///
/// * `Result<(), anyhow::Error>` - The result of the operation
///
async fn handle_ws(mut ws: WebSocket<TokioIo<Upgraded>>)
    -> Result<(), anyhow::Error> {

    // loop reading messages
    loop {

        // try to read a message
        let msg = ws.read_frame()
            .await.context("failed to read message from websocket")?;

        // handle message
        match msg.opcode {
            // spec states only text frames are allowed
            OpCode::Continuation | OpCode::Binary | OpCode::Ping | OpCode::Pong => {
                warn!(target: "jetstream", "unexpected opcode: {:?}", msg.opcode);
            },
            // can be emitted by the server
            OpCode::Close => {
                anyhow::bail!("unexpected connection close: {:?}", msg.payload);
            },
            // handle text message
            OpCode::Text => {
                rayon::spawn(move || { handle_text(msg); });
            }
        };
    }
}

///
/// Handle a text message
/// NOTE: This function is run in multiple threads!
///
/// # Arguments
///
/// * `ws` - The websocket connection
/// * `msg` - The message to handle
///
fn handle_text(msg: Frame<'_>) {
    // decode message
    let text = String::from_utf8(msg.payload.to_vec())
        .unwrap(); // it's fine to panic here, because something is very wrong if this fails

    // parse message
    let event = events::parse_event(text);
    if event.is_err() {
        warn!(target: "jetstream", "failed to parse event: {:?}", event.err().unwrap());
        return;
    }
    let event = event.unwrap();

    // update cursor if possible
    {
        let time = match event {
            Kind::CommitEvent { time_us, .. } => time_us,
            Kind::IdentityEvent { time_us, .. } => time_us,
            Kind::KeyEvent { time_us, .. } => time_us
        };

        // instead of unwrapping, we don't care if the variable is locked
        // as the very next post a few microseconds later will update it.
        // however, if this lock is poisoned, we will never update the cursor!!
        if let Ok(mut current_cursor) = CURSOR.write() {
            *current_cursor = time;
        }
    }
}
