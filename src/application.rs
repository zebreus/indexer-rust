use std::{sync::{LazyLock, RwLock}, time::{Duration, Instant}};

use anyhow::Context;
use events::Kind;
use fastwebsockets::{Frame, OpCode, WebSocket};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use log::{error, info, warn};
use surrealdb::{RecordId, Surreal};
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

/// Cursor to keep track of the current position in the stream
static CURSOR: LazyLock<RwLock<u64>> = LazyLock::new(|| RwLock::new(0));
/// Various metrics
static READ_ERRORS: LazyLock<RwLock<u64>> = LazyLock::new(|| RwLock::new(0));
static PARSE_ERRORS: LazyLock<RwLock<u64>> = LazyLock::new(|| RwLock::new(0));
static SUCCESSFUL_EVENTS: LazyLock<RwLock<u64>> = LazyLock::new(|| RwLock::new(0));

static DB: LazyLock<Surreal<surrealdb::engine::remote::ws::Client>> = LazyLock::new(Surreal::init);

fn try_increase_counter(counter: &LazyLock<RwLock<u64>>) {
    if let Ok(mut counter) = counter.write() {
        *counter += 1;
    }
}

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
pub async fn launch_client(
    host: &String,
    cert: &String,
    initial_cursor: u64,
) -> Result<(), anyhow::Error> {
    // connect to the database
    connect_to_db().await?;

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
            try_increase_counter(&READ_ERRORS);
            error!(target: "jetstream", "error handling websocket: {:?}", res.err().unwrap());
        }

        // rewind cursor by 2 seconds
        {
            let mut cursor = CURSOR.write().unwrap();
            *cursor -= 2;
            info!(target: "jetstream", "rewinding cursor by 2 seconds: {} -> {}", cursor, *cursor);
        }

        // give the server 200 ms to recover
        sleep(Duration::from_millis(200)).await;
    }
}

async fn connect_to_db() -> anyhow::Result<()> {
    // Connect to the database
    DB.connect::<surrealdb::engine::remote::ws::Ws>("127.0.0.1:8000")
        .await?;

    // Sign in to the server
    DB.signin(surrealdb::opt::auth::Root {
        username: "root",
        password: "root",
    })
    .await?;

    DB.query("DEFINE NAMESPACE atp;").await?;
    DB.use_ns("atp").await?;

    DB.query("DEFINE DATABASE atp;").await?;
    DB.use_ns("atp").use_db("atp").await?;

    // TODO Add all types
    DB.query(
        "
DEFINE TABLE did SCHEMAFULL;
DEFINE FIELD handle ON TABLE did TYPE option<string>;
DEFINE FIELD displayName ON TABLE did TYPE option<string>;
DEFINE FIELD description ON TABLE did TYPE option<string>;
DEFINE FIELD avatar ON TABLE did TYPE option<record<blob>>;
DEFINE FIELD banner ON TABLE did TYPE option<record<blob>>;
DEFINE FIELD labels ON TABLE did TYPE option<array>;
DEFINE FIELD labels.* ON TABLE did TYPE string;
DEFINE FIELD joinedViaStarterPack ON TABLE did TYPE option<record<starterpack>>;
DEFINE FIELD pinnedPost ON TABLE did TYPE option<record<post>>;
DEFINE FIELD createdAt ON TABLE did TYPE datetime;

DEFINE TABLE post SCHEMAFULL;
DEFINE FIELD text ON TABLE post TYPE string;
", // record<one | two>
    )
    .await?;

    Ok(())
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
    let mut now = Instant::now();

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

        // print metrics every 60 seconds
        if now.elapsed().as_secs() >= 60 {
            let read_errors = {
                let read_errors = READ_ERRORS.read().unwrap();
                *read_errors
            };
            let parse_errors = {
                let parse_errors = PARSE_ERRORS.read().unwrap();
                *parse_errors
            };
            let successful_events = {
                let successful_events = SUCCESSFUL_EVENTS.read().unwrap();
                *successful_events
            };

            info!(target: "jetstream", "read errors: {}, parse errors: {}, successful events: {}",
                  read_errors, parse_errors, successful_events);

            now = Instant::now();
        }
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
        try_increase_counter(&PARSE_ERRORS);
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

    try_increase_counter(&SUCCESSFUL_EVENTS);
}
