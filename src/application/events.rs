use anyhow::Context;
use atrium_api::{record::KnownRecord, types::string::{Did, Handle, RecordKey}};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
#[serde(tag = "operation")]
pub enum Commit {
    #[serde(rename = "create")]
    Create {
        rev: String,
        collection: String,
        rkey: RecordKey,
        record: KnownRecord,
        cid: String
    },
    #[serde(rename = "update")]
    Update {
        // fields are speculated, not documented:
        rev: String,
        collection: String,
        rkey: RecordKey,
        record: KnownRecord,
        cid: String
    },
    #[serde(rename = "delete")]
    Delete {
        rev: String,
        collection: String,
        rkey: RecordKey
    }
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub struct Identity {
    pub did: Did,
    pub handle: Handle,
    pub seq: u64,
    pub time: String
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub struct Account {
    pub active: bool,
    pub did: Did,
    pub seq: u64,
    pub time: String
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
#[serde(tag = "kind")]
pub enum Kind {
    #[serde(rename = "commit")]
    CommitEvent {
        did: Did,
        time_us: u64,
        commit: Commit
    },
    #[serde(rename = "identity")]
    IdentityEvent {
        did: Did,
        time_us: u64,
        identity: Identity
    },
    #[serde(rename = "account")]
    KeyEvent {
        did: Did,
        time_us: u64,
        account: Account
    },
}

///
/// Parse an event from a string
///
/// # Arguments
///
/// * `msg` - The string to parse
///
/// # Returns
///
/// The parsed event
///
/// # Errors
///
/// If the event could not be parsed
///
/// # Safety
///
/// This function is unsafe because it uses `simd-json` to parse the event
///
pub fn parse_event(mut msg: String) -> Result<Kind, anyhow::Error> {
    let event: Kind = unsafe { simd_json::from_str(msg.as_mut_str()) }
        .context("failed to parse event")?;
    Ok(event)
}