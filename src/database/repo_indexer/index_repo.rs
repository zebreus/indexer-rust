use crate::{
    config::ARGS,
    database::{
        definitions::Record,
        handlers::{on_commit_event_createorupdate, BigUpdate},
        repo_indexer::pipeline::NoNextStage,
    },
};
use atrium_api::{
    record::KnownRecord,
    types::string::{Did, RecordKey},
};
// use ipld_core::cid::{Cid, CidGeneric};
use reqwest::Client;
use serde::Deserialize;
use serde_ipld_dagcbor::from_reader;
use std::{
    collections::HashMap,
    future::Future,
    io::Read,
    string::FromUtf8Error,
    sync::LazyLock,
    time::{Duration, Instant},
};
use surrealdb::{engine::any::Any, opt::PatchOp, RecordId, Surreal};
use tokio::task::spawn_blocking;
use tracing::{info, instrument, span, trace, warn, Level, Span};

use super::pipeline::Stage;

type Cid = ipld_core::cid::Cid;

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
fn insert_into_map(
    mut files: HashMap<ipld_core::cid::Cid, Vec<u8>>,
    file: (Cid, Vec<u8>),
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

/// Get the plc response service for the repo
#[instrument(skip_all)]
async fn get_plc_service(
    http_client: &Client,
    did: &str,
) -> anyhow::Result<Option<PlcDirectoryDidResponseService>> {
    let resp = http_client
        .get(format!("https://plc.directory/{}", did))
        .timeout(Duration::from_secs(ARGS.directory_download_timeout))
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
) -> anyhow::Result<Vec<u8>> {
    let get_repo_response = REQWEST_CLIENT
        .get(format!(
            "{}/xrpc/com.atproto.sync.getRepo?did={}",
            service.service_endpoint, did,
        ))
        .timeout(tokio::time::Duration::from_secs(ARGS.repo_download_timeout))
        .send()
        .await?;
    let bytes = get_repo_response.bytes().await?.to_vec();
    info!(
        "Downloaded repo {} with size {:.2} MB",
        did,
        bytes.len() as f64 / (1000.0 * 1000.0)
    );
    return Ok(bytes);
}

/// Download the file for the given repo into a map
#[instrument(skip_all)]
fn deserialize_repo(mut bytes: Vec<u8>) -> anyhow::Result<HashMap<Cid, Vec<u8>>> {
    let (entries, header) = rs_car_sync::car_read_all(&mut bytes.as_slice(), true)?;
    // let car_reader = CarReader::new(bytes.as_ref()).await?;
    let files = entries
        .into_iter()
        .map(|(cid, data)| {
            let cid_bytes = cid.to_bytes();
            let cid: Cid = ipld_core::cid::Cid::read_bytes(cid_bytes.as_slice()).unwrap();
            (cid, data)
        })
        .try_fold(HashMap::new(), insert_into_map);

    files
}

/// Apply updates to the database
#[instrument(skip_all)]
fn create_big_update(did: &str, updates: Vec<DatabaseUpdate>) -> anyhow::Result<BigUpdate> {
    let did_key = crate::database::utils::did_to_key(did)?;
    let did = did.to_owned();
    let did_key = did_key.to_owned();

    let mut db_updates = updates.into_iter().map(|update| {
        let did_key = did_key.clone();
        let did = did.to_string();

        let res = on_commit_event_createorupdate(
            Did::new(did.clone().into()).unwrap(),
            did_key,
            update.collection,
            update.rkey,
            update.record,
        );

        match res {
            Ok(big_update) => {
                return Ok(big_update);
            }
            Err(e) => {
                warn!("on_commit_event_createorupdate {} {}", e, did);
                return Err(e);
            }
        }
    });
    let mut really_big_update = BigUpdate::default();
    loop {
        let Some(result) = db_updates.next() else {
            break;
        };
        match result {
            Ok(big_update) => {
                really_big_update.merge(big_update);
            }
            Err(e) => {
                warn!("Failed to apply update: {}", e);
                return Err(e);
            }
        }
    }
    Ok(really_big_update)
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

/// Has a service
pub struct WithService {
    service: PlcDirectoryDidResponseService,
    // TODO: Figure out why now is created this early
    now: std::time::Duration,
}
/// Has files
pub struct WithRepo {
    now: std::time::Duration,
    repo: Vec<u8>,
}

/// Has converted the files to update
pub struct WithUpdates {
    now: std::time::Duration,
    pub update: BigUpdate,
}

pub struct PipelineItem<State> {
    db: Surreal<Any>,
    http_client: Client,
    did: String,
    span: Span,
    pub state: State,
}

impl PipelineItem<New> {
    pub fn new(db: Surreal<Any>, http_client: Client, did: String) -> PipelineItem<New> {
        let span = span!(target: "backfill", parent: None, Level::INFO, "pipeline_item");
        span.record("did", did.clone());
        span.in_scope(|| {
            trace!("Start backfilling repo");
        });
        PipelineItem::<New> {
            db,
            http_client,
            did,
            span,
            state: New {},
        }
    }
}

impl Stage for PipelineItem<New> {
    type Output = PipelineItem<WithService>;
    const NAME: &str = "download_information";
    // type F = Future<Output = anyhow::Result<Self::Output>> + Send + Sync + 'static;

    fn run(self) -> impl Future<Output = anyhow::Result<Self::Output>> + Send + Sync + 'static {
        return async {
            let service = get_plc_service(&self.http_client, &self.did).await?;
            let Some(service) = service else {
                // TODO: Handle this better, as this is not really an error
                return Err(anyhow::anyhow!("Failed to get a plc service"));
            };
            Ok(PipelineItem::<WithService> {
                state: WithService {
                    service: service,
                    now: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap(),
                },
                db: self.db,
                http_client: self.http_client,
                did: self.did,
                span: self.span,
            })
        };
    }
}

impl Stage for PipelineItem<WithService> {
    type Output = PipelineItem<WithRepo>;
    const NAME: &str = "download_repo";
    // type F = Future<Output = anyhow::Result<Self::Output>> + Send + Sync + 'static;

    fn run(self) -> impl Future<Output = anyhow::Result<Self::Output>> + Send + Sync + 'static {
        return async move {
            let repo = download_repo(&self.state.service, &self.did).await?;
            Ok(PipelineItem::<WithRepo> {
                state: WithRepo {
                    now: self.state.now,
                    repo,
                },
                db: self.db,
                http_client: self.http_client,
                did: self.did,
                span: self.span,
            })
        };
    }
}

impl Stage for PipelineItem<WithRepo> {
    type Output = PipelineItem<WithUpdates>;
    const NAME: &str = "process_repo";
    // type F = Future<Output = anyhow::Result<Self::Output>> + Send + Sync + 'static;

    fn run(self) -> impl Future<Output = anyhow::Result<Self::Output>> + Send + Sync + 'static {
        return async move {
            // info!("Deserializing repo {}", self.did);
            let did = self.did.clone();
            let big_update = spawn_blocking(move || {
                let files: HashMap<ipld_core::cid::CidGeneric<64>, Vec<u8>> =
                    deserialize_repo(self.state.repo)?;
                let updates = files_to_updates_blocking(files)?;
                let mut big_update = create_big_update(&did, updates)?;

                big_update.add_timestamp(&did, surrealdb::sql::Datetime::from(chrono::Utc::now()));
                Result::<BigUpdate, anyhow::Error>::Ok(big_update)
            })
            .await??;

            Ok(PipelineItem::<WithUpdates> {
                state: WithUpdates {
                    now: self.state.now,
                    update: big_update,
                },
                db: self.db,
                http_client: self.http_client,
                did: self.did,
                span: self.span,
            })
        };
    }
}

impl Stage for PipelineItem<WithUpdates> {
    type Output = NoNextStage;
    const NAME: &str = "apply_updates";
    // type F = Future<Output = anyhow::Result<Self::Output>> + Send + Sync + 'static;

    fn run(self) -> impl Future<Output = anyhow::Result<Self::Output>> + Send + Sync + 'static {
        return async move {
            let start = Instant::now();

            if !ARGS.dont_write_when_backfilling.unwrap_or(false) {
                self.state.update.apply(&self.db).await?;
            } else {
                eprintln!("Skipping writing to the database and sleeping instead");
                std::thread::sleep(Duration::from_secs(1));
            }
            let duration = start.elapsed();
            eprintln!("Big update took {:?}", duration);
            warn!("Big update took {:?}", duration);
            // let _: Option<Record> = &self
            //     .db
            //     .update(("latest_backfill", did_key.clone()))
            //     .patch(PatchOp::replace(
            //         "at",
            //         surrealdb::sql::Datetime::from(chrono::Utc::now()),
            //     ))
            //     .await?;
            Ok(NoNextStage {})
        };
    }
}

// impl Stage for PipelineItem<Done> {
//     type Output = PipelineItem<Done>;
//     const NAME: &str = "done";
//     // type F = Future<Output = anyhow::Result<Self::Output>> + Send + Sync + 'static;

//     fn run(self) -> impl Future<Output = anyhow::Result<Self::Output>> + Send + Sync + 'static {
//         return async move {
//             trace!("Indexed {}", self.did);
//             Ok(self)
//         };
//     }
// }

// impl PipelineItem<New> {
//     #[instrument(skip(self), parent = &self.span)]
//     pub async fn get_service(self) -> anyhow::Result<PipelineItem<WithService>> {
//         let service = get_plc_service(&self.http_client, &self.did).await?;
//         let Some(service) = service else {
//             // TODO: Handle this better, as this is not really an error
//             return Err(anyhow::anyhow!("Failed to get a plc service"));
//         };
//         Ok(PipelineItem::<WithService> {
//             state: WithService {
//                 service: service,
//                 now: std::time::SystemTime::now()
//                     .duration_since(std::time::UNIX_EPOCH)
//                     .unwrap(),
//             },
//             db: self.db,
//             http_client: self.http_client,
//             did: self.did,
//             span: self.span,
//         })
//     }
// }

impl PipelineItem<WithService> {
    #[instrument(skip(self), parent = &self.span)]
    pub async fn download_repo(self) -> anyhow::Result<PipelineItem<WithRepo>> {
        let repo = download_repo(&self.state.service, &self.did).await?;
        Ok(PipelineItem::<WithRepo> {
            state: WithRepo {
                now: self.state.now,
                repo,
            },
            db: self.db,
            http_client: self.http_client,
            did: self.did,
            span: self.span,
        })
    }
}

impl PipelineItem<WithRepo> {
    #[instrument(skip(self), parent = &self.span)]
    pub async fn process_repo(self) -> anyhow::Result<PipelineItem<WithUpdates>> {
        // info!("Deserializing repo {}", self.did);
        let did = self.did.clone();
        let big_update = spawn_blocking(move || {
            let files: HashMap<ipld_core::cid::CidGeneric<64>, Vec<u8>> =
                deserialize_repo(self.state.repo)?;
            let updates = files_to_updates_blocking(files)?;
            let mut big_update = create_big_update(&did, updates)?;

            big_update.add_timestamp(&did, surrealdb::sql::Datetime::from(chrono::Utc::now()));
            Result::<BigUpdate, anyhow::Error>::Ok(big_update)
        })
        .await??;

        Ok(PipelineItem::<WithUpdates> {
            state: WithUpdates {
                now: self.state.now,
                update: big_update,
            },
            db: self.db,
            http_client: self.http_client,
            did: self.did,
            span: self.span,
        })
    }
}

impl PipelineItem<WithUpdates> {
    #[instrument(skip(self), parent = &self.span)]
    pub async fn apply_updates(self) -> anyhow::Result<PipelineItem<NoNextStage>> {
        let start = Instant::now();

        if !ARGS.dont_write_when_backfilling.unwrap_or(false) {
            self.state.update.apply(&self.db).await?;
        } else {
            eprintln!("Skipping writing to the database and sleeping instead");
            std::thread::sleep(Duration::from_secs(1));
        }
        let duration = start.elapsed();
        eprintln!("Big update took {:?}", duration);
        warn!("Big update took {:?}", duration);
        // let _: Option<Record> = &self
        //     .db
        //     .update(("latest_backfill", did_key.clone()))
        //     .patch(PatchOp::replace(
        //         "at",
        //         surrealdb::sql::Datetime::from(chrono::Utc::now()),
        //     ))
        //     .await?;
        Ok(PipelineItem::<NoNextStage> {
            state: NoNextStage {},
            db: self.db,
            http_client: self.http_client,
            did: self.did,
            span: self.span,
        })
    }
}

impl PipelineItem<NoNextStage> {
    #[instrument(skip(self), parent = &self.span)]
    pub async fn print_report(self) -> anyhow::Result<()> {
        // TODO: This is only for printing debug stuff
        trace!("Indexed {}", self.did);
        Ok(())
    }
}
