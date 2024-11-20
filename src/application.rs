use anyhow::Context;
use fastwebsockets::{OpCode, WebSocket};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use log::{error, info, warn};

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
pub async fn launch_client(host: &String, cert: &String, initial_cursor: Option<&u64>) ->
    Result<(), anyhow::Error> {

    // loop infinitely, ensuring connection aborts are handled
    loop {

        // TODO: if the connection fails, the cursor should be rewinded to the last received position minus
        // a certain amount of time, to ensure no data is lost.

        // create a new connection
        let ws =
           ws::connect(host, cert, initial_cursor, NSIDS.to_vec())
            .await.context("failed to establish connection to jetstream")?;
        info!(target: "jetstream", "established new connection to jetstream server");

        let res = handle_ws(ws).await;
        if res.is_err() {
            error!(target: "jetstream", "error handling websocket: {:?}", res.err().unwrap());
        }

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
