use super::LastIndexedTimestamp;
use crate::database::{definitions::Record, handlers::on_commit_event_createorupdate};
use atrium_api::{
    record::KnownRecord,
    types::string::{Did, RecordKey},
};
use futures::{stream::FuturesUnordered, TryStreamExt};
use hyper::body::Bytes;
use ipld_core::cid::{Cid, CidGeneric};
use iroh_car::CarReader;
use reqwest::Client;
use serde::Deserialize;
use serde_ipld_dagcbor::from_reader;
use std::{collections::HashMap, string::FromUtf8Error, sync::LazyLock, time::Duration};
use surrealdb::{engine::any::Any, Surreal};
use tokio::task::spawn_blocking;
use tracing::{info, instrument, span, trace, warn, Level, Span};

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

pub struct DatabaseUpdate {
    collection: String,
    rkey: RecordKey,
    record: KnownRecord,
}

/// Insert a file into a map
async fn insert_into_map(
    mut files: HashMap<ipld_core::cid::Cid, Vec<u8>>,
    file: (CidGeneric<64>, Vec<u8>),
) -> anyhow::Result<HashMap<ipld_core::cid::Cid, Vec<u8>>> {
    let (cid, data) = file;
    files.insert(cid, data);
    Ok(files)
}

/// Convert downloaded files into database updates. Blocks the thread
#[instrument(skip_all)]
fn files_to_updates_blocking(
    files: HashMap<Cid, Vec<u8>>,
) -> Result<Vec<DatabaseUpdate>, FromUtf8Error> {
    // TODO: Understand this logic and whether this can be done streaming
    let mut result = Vec::new();
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

            let update = DatabaseUpdate {
                collection: parts.next().unwrap().to_string(),
                rkey: RecordKey::new(parts.next().unwrap().to_string()).unwrap(),
                record,
            };
            result.push(update);
        }
    }
    return Ok(result);
}

/// Check if a repo is already indexed
#[instrument()]
async fn check_indexed(db: &Surreal<Any>, did: &str) -> anyhow::Result<bool> {
    let did_key = crate::database::utils::did_to_key(did)?;

    Ok(db
        .select::<Option<LastIndexedTimestamp>>(("li_did", &did_key))
        .await?
        .is_some())
}

/// Get the plc response service for the repo
#[instrument(skip_all)]
async fn get_plc_service(
    http_client: &Client,
    did: &str,
) -> anyhow::Result<Option<PlcDirectoryDidResponseService>> {
    let resp = http_client
        .get(format!("https://plc.directory/{}", did))
        .send()
        .await?
        .json::<PlcDirectoryDidResponse>()
        .await?;
    let service = resp.service.into_iter().next();
    Ok(service)
}

/// Download a repo from the given service
#[instrument(skip_all)]
async fn download_repo(
    service: &PlcDirectoryDidResponseService,
    did: &str,
) -> anyhow::Result<Bytes> {
    let get_repo_response = REQWEST_CLIENT
        .get(format!(
            "{}/xrpc/com.atproto.sync.getRepo?did={}",
            service.service_endpoint, did,
        ))
        .send()
        .await?;
    let bytes = get_repo_response.bytes().await?;
    info!(
        "Downloaded repo {} with size {:.2} MB",
        did,
        bytes.len() as f64 / (1000.0 * 1000.0)
    );
    return Ok(bytes);
}

/// Download the file for the given repo into a map
#[instrument(skip_all)]
async fn deserialize_repo(bytes: Bytes) -> anyhow::Result<HashMap<Cid, Vec<u8>>> {
    // TODO: Benchmark CarReader. This is probably not the right place for parsing logic
    let car_reader = CarReader::new(bytes.as_ref()).await?;
    let files = car_reader
        .stream()
        .map_err(|e| e.into())
        .try_fold(HashMap::new(), insert_into_map)
        .await;

    files
}

/// Convert downloaded files into database updates
#[instrument(skip_all)]
async fn files_to_updates(files: HashMap<Cid, Vec<u8>>) -> anyhow::Result<Vec<DatabaseUpdate>> {
    // TODO: Look into using block_in_place instead of spawn_blocking
    let result = spawn_blocking(|| files_to_updates_blocking(files)).await??;
    Ok(result)
}

/// Apply updates to the database
#[instrument(skip_all)]
async fn apply_updates(
    db: &Surreal<Any>,
    did: &str,
    updates: Vec<DatabaseUpdate>,
    update_timestamp: &Duration,
) -> anyhow::Result<()> {
    let did_key = crate::database::utils::did_to_key(did)?;

    let futures: Vec<_> = updates
        .into_iter()
        .map(|update| {
            let db = db.clone();
            let did_key = did_key.clone();
            let did = did.to_string();
            tokio::spawn(async move {
                let res = on_commit_event_createorupdate(
                    &db,
                    Did::new(did.clone().into()).unwrap(),
                    did_key,
                    update.collection,
                    update.rkey,
                    update.record,
                )
                .await;

                if let Err(error) = res {
                    warn!("on_commit_event_createorupdate {} {}", error, did);
                }
            })
        })
        .collect();
    for f in futures.into_iter() {
        f.await;
    }

    let _: Option<Record> = db
        .upsert(("li_did", did_key))
        .content(LastIndexedTimestamp {
            time_us: update_timestamp.as_micros() as u64,
            time_dt: chrono::Utc::now().into(),
            error: None,
        })
        .await?;
    Ok(())
}

// /// Indexes the repo with the given DID (Decentralized Identifier)
// async fn index_repo(db: &Surreal<Any>, http_client: &Client, did: &String) -> anyhow::Result<()> {
//     {
//         if check_indexed(&db, &did).await? {
//             return Ok(());
//         }
//     }

//     let now = std::time::SystemTime::now()
//         .duration_since(std::time::UNIX_EPOCH)
//         .unwrap();

//     let service = {
//         let Some(service) = get_plc_service(&http_client, &did).await? else {
//             return Ok(());
//         };
//         service
//     };

//     let repo = { download_repo(&service, &did).await? };
//     let files = { deserialize_repo(repo).await? };

//     let updates = { files_to_updates(files).await? };
//     let update_result = { apply_updates(&db, &did, updates, &now).await? };
//     Ok(())
// }

/// No processing has been done on this item
pub struct New {}

/// It was verified that the item is not indexed yet
pub struct NotIndexed {}
/// Has a service
pub struct WithService {
    service: PlcDirectoryDidResponseService,
    // TODO: Figure out why now is created this early
    now: std::time::Duration,
}
/// Has files
pub struct WithRepo {
    now: std::time::Duration,
    repo: Bytes,
}

pub struct WithFiles {
    now: std::time::Duration,
    files: HashMap<ipld_core::cid::Cid, Vec<u8>>,
}
/// Has converted the files to update
pub struct WithUpdates {
    now: std::time::Duration,
    pub updates: Vec<DatabaseUpdate>,
}
/// Updates have been applied
pub struct Done {}

pub struct PipelineItem<'a, State> {
    db: &'a Surreal<Any>,
    http_client: &'a Client,
    did: String,
    span: Span,
    pub state: State,
}

impl<'a> PipelineItem<'a, New> {
    pub fn new(
        db: &'a Surreal<Any>,
        http_client: &'a Client,
        did: String,
    ) -> PipelineItem<'a, New> {
        let span = span!(target: "backfill", parent: None, Level::INFO, "pipeline_item");
        span.record("did", did.clone());
        span.in_scope(|| {
            trace!("Start backfilling repo");
        });
        PipelineItem::<'a, New> {
            db,
            http_client,
            did,
            span,
            state: New {},
        }
    }
}

impl<'a> PipelineItem<'a, New> {
    #[instrument(skip(self), parent = &self.span)]
    pub async fn check_indexed(self) -> anyhow::Result<PipelineItem<'a, NotIndexed>> {
        if check_indexed(&self.db, &self.did).await? {
            // TODO: Handle this better, as this is not really an error
            return Err(anyhow::anyhow!("Already indexed"));
        }
        Ok(PipelineItem::<'a, NotIndexed> {
            state: NotIndexed {},
            ..self
        })
    }
}

impl<'a> PipelineItem<'a, NotIndexed> {
    #[instrument(skip(self), parent = &self.span)]
    pub async fn get_service(self) -> anyhow::Result<PipelineItem<'a, WithService>> {
        let service = get_plc_service(&self.http_client, &self.did).await?;
        let Some(service) = service else {
            // TODO: Handle this better, as this is not really an error
            return Err(anyhow::anyhow!("Failed to get a plc service"));
        };
        Ok(PipelineItem::<'a, WithService> {
            state: WithService {
                service: service,
                now: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap(),
            },
            ..self
        })
    }
}

impl<'a> PipelineItem<'a, WithService> {
    #[instrument(skip(self), parent = &self.span)]
    pub async fn download_repo(self) -> anyhow::Result<PipelineItem<'a, WithRepo>> {
        let repo = download_repo(&self.state.service, &self.did).await?;
        Ok(PipelineItem::<'a, WithRepo> {
            state: WithRepo {
                now: self.state.now,
                repo,
            },
            ..self
        })
    }
}

impl<'a> PipelineItem<'a, WithRepo> {
    #[instrument(skip(self), parent = &self.span)]
    pub async fn deserialize_repo(self) -> anyhow::Result<PipelineItem<'a, WithFiles>> {
        info!("Deserializing repo {}", self.did);
        let files = deserialize_repo(self.state.repo).await?;
        Ok(PipelineItem::<'a, WithFiles> {
            state: WithFiles {
                now: self.state.now,
                files,
            },
            ..self
        })
    }
}

impl<'a> PipelineItem<'a, WithFiles> {
    #[instrument(skip(self), parent = &self.span)]
    pub async fn files_to_updates(self) -> anyhow::Result<PipelineItem<'a, WithUpdates>> {
        let updates = files_to_updates(self.state.files).await?;
        Ok(PipelineItem::<'a, WithUpdates> {
            state: WithUpdates {
                now: self.state.now,
                updates,
            },
            ..self
        })
    }
}

impl<'a> PipelineItem<'a, WithUpdates> {
    #[instrument(skip(self), parent = &self.span)]
    pub async fn apply_updates(self) -> anyhow::Result<PipelineItem<'a, Done>> {
        apply_updates(&self.db, &self.did, self.state.updates, &self.state.now).await?;
        Ok(PipelineItem::<'a, Done> {
            state: Done {},
            ..self
        })
    }
}

impl<'a> PipelineItem<'a, Done> {
    #[instrument(skip(self), parent = &self.span)]
    pub async fn print_report(self) -> () {
        // TODO: This is only for printing debug stuff
        trace!("Indexed {}", self.did);
    }
}
