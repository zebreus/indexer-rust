use anyhow::Context;
use atrium_api::{
    record::KnownRecord,
    types::string::{Did, Handle, RecordKey},
};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
#[serde(tag = "operation")]
pub enum Commit {
    #[serde(rename = "create", alias = "update")]
    CreateOrUpdate {
        rev: String,
        collection: String,
        rkey: RecordKey,
        record: KnownRecord,
        cid: String,
    },
    #[serde(rename = "delete")]
    Delete {
        rev: String,
        collection: String,
        rkey: RecordKey,
    },
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub struct Identity {
    pub did: Did,
    pub handle: Handle,
    pub seq: u64,
    pub time: String,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub struct Account {
    pub active: bool,
    pub did: Did,
    pub seq: u64,
    pub time: String,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
#[serde(tag = "kind")]
pub enum Kind {
    #[serde(rename = "commit")]
    Commit {
        did: Did,
        time_us: i64,
        commit: Commit,
    },
    #[serde(rename = "identity")]
    Identity {
        did: Did,
        time_us: i64,
        identity: Identity,
    },
    #[serde(rename = "account")]
    Key {
        did: Did,
        time_us: i64,
        account: Account,
    },
}

/// Parse an event from a string
pub fn parse_event(mut msg: String) -> anyhow::Result<Kind> {
    unsafe { simd_json::from_str(msg.as_mut_str()) }.context("Failed to parse event")
}
