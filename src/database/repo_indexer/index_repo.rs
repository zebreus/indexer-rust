use super::{LastIndexedTimestamp, SharedState};
use crate::database::{definitions::Record, handlers::on_commit_event_createorupdate};
use atrium_api::{
    record::KnownRecord,
    types::string::{Did, RecordKey},
};
use futures::TryStreamExt;
use ipld_core::cid::{Cid, CidGeneric};
use iroh_car::CarReader;
use log::warn;
use reqwest::Client;
use serde::Deserialize;
use serde_ipld_dagcbor::from_reader;
use std::{
    collections::BTreeMap,
    sync::{Arc, LazyLock},
};

/// There should only be one request client to make use of connection pooling
// TODO: Dont use a global client
static REQWEST_CLIENT: LazyLock<Client> = LazyLock::new(|| Client::new());

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct PlcDirectoryDidResponse {
    #[serde(rename = "alsoKnownAs")]
    also_known_as: Vec<String>,
    service: Vec<PlcDirectoryDidResponseService>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct PlcDirectoryDidResponseService {
    #[serde(rename = "serviceEndpoint")]
    service_endpoint: String,
    #[serde(rename = "type")]
    type_: String,
    id: String,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
/// https://atproto.com/specs/repository
pub struct TreeEntry {
    #[serde(rename = "p")]
    /// Count of bytes shared with previous TreeEntry in this Node (if any)
    pub prefix_len: u8,
    #[serde(with = "serde_bytes", rename = "k")]
    /// Remainder of key for this TreeEntry, after "prefixlen" have been removed
    pub key_suffix: Vec<u8>,
    #[serde(rename = "v")]
    /// Link to the record data (CBOR) for this entry
    pub value: Cid,
    #[serde(rename = "t")]
    /// Link to a sub-tree Node at a lower level which has keys sorting after this TreeEntry's key (to the "right"), but before the next TreeEntry's key in this Node (if any)
    pub tree: Option<Cid>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
/// https://atproto.com/specs/repository
pub struct NodeData {
    #[serde(rename = "l")]
    /// Link to sub-tree Node on a lower level and with all keys sorting before keys at this node
    pub left: Option<Cid>,
    #[serde(rename = "e")]
    /// All the entries in the node
    pub entries: Vec<TreeEntry>,
}

/// Insert a file into a map
async fn insert_into_map(
    mut files: BTreeMap<ipld_core::cid::Cid, Vec<u8>>,
    file: (CidGeneric<64>, Vec<u8>),
) -> anyhow::Result<BTreeMap<ipld_core::cid::Cid, Vec<u8>>> {
    let (cid, data) = file;
    files.insert(cid, data);
    Ok(files)
}

/// Get the contents of a repo with the given DID (Decentralized Identifier)
async fn get_files(
    service: &PlcDirectoryDidResponseService,
    did: &str,
) -> anyhow::Result<BTreeMap<ipld_core::cid::Cid, Vec<u8>>> {
    let car_res = REQWEST_CLIENT
        .get(format!(
            "{}/xrpc/com.atproto.sync.getRepo?did={}",
            service.service_endpoint, did,
        ))
        .send()
        .await?;
    let car_res_bytes = car_res.bytes().await?;

    let buf_reader = tokio::io::BufReader::new(&car_res_bytes[..]);

    // TODO: Benchmark CarReader. This is probably not the right place for parsing logic
    let car_reader = CarReader::new(buf_reader).await?;

    let files = car_reader
        .stream()
        .map_err(|e| e.into())
        .try_fold(BTreeMap::new(), insert_into_map)
        .await;

    files
}

/// Indexes the repo with the given DID (Decentralized Identifier)
pub async fn index_repo(state: &Arc<SharedState>, did: &String) -> anyhow::Result<()> {
    let did_key = crate::database::utils::did_to_key(did.as_str())?;

    if state
        .db
        .select::<Option<LastIndexedTimestamp>>(("li_did", &did_key))
        .await?
        .is_some()
    {
        // debug!("skip {}", did);
        return Ok(());
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros();

    let resp = state
        .http_client
        .get(format!("https://plc.directory/{}", did))
        .send()
        .await?
        .json::<PlcDirectoryDidResponse>()
        .await?;

    let Some(service) = resp.service.first() else {
        return Ok(());
    };
    let files = get_files(service, did).await?;

    for file in &files {
        let Ok(node_data) = from_reader::<NodeData, _>(&file.1[..]) else {
            continue;
        };
        let mut key = "".to_string();
        for entry in node_data.entries {
            let k = String::from_utf8(entry.key_suffix)?;
            key = format!("{}{}", key.split_at(entry.prefix_len as usize).0, k);

            let Some(block) = files.get(&entry.value) else {
                continue;
            };

            let Ok(record) = from_reader::<KnownRecord, _>(&block[..]) else {
                continue;
            };

            let mut parts = key.split("/");

            let res = on_commit_event_createorupdate(
                &state.db,
                Did::new(did.clone()).unwrap(),
                did_key.clone(),
                parts.next().unwrap().to_string(),
                RecordKey::new(parts.next().unwrap().to_string()).unwrap(),
                record,
            )
            .await;

            if let Err(error) = res {
                warn!("on_commit_event_createorupdate {} {}", error, did);
            }
        }
    }
    let _: Option<Record> = state
        .db
        .upsert(("li_did", did_key))
        .content(LastIndexedTimestamp {
            time_us: now as u64,
            time_dt: chrono::Utc::now().into(),
            error: None,
        })
        .await?;
    Ok(())
}
