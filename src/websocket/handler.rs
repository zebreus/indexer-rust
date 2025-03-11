use anyhow::Context;

use crate::database::{self, definitions::JetstreamCursor};

use super::{events, SharedState};

/// Handle a message from the websocket in parallel
pub async fn handle_message(
    state: &SharedState,
    msg: String,
    update_cursor: bool,
) -> anyhow::Result<()> {
    // parse event
    let event = events::parse_event(msg)?;

    // update cursor
    let time = match &event {
        events::Kind::Commit { time_us, .. } => *time_us,
        events::Kind::Identity { time_us, .. } => *time_us,
        events::Kind::Key { time_us, .. } => *time_us,
    } as i64;
    state.update_cursor(time);
    if update_cursor {
        database::write_cursor(
            &state.database.clone(),
            JetstreamCursor {
                host: state.host.clone(),
                time_us: time,
            },
        )
        .await
        .context("Unable to write cursor to database!")?;
    }

    database::handlers::handle_event(state.database.clone(), event)
        .await
        .context("Unable to handle event")?;

    Ok(())
}
