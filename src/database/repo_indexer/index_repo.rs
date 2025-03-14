use super::pipeline::Stage;
use crate::{
    config::ARGS,
    database::{
        big_update::{create_big_update, BigUpdate},
        repo_indexer::pipeline::NoNextStage,
    },
};
use atrium_api::{
    record::KnownRecord,
    types::string::{Did, RecordKey},
};
use chrono::{DateTime, Utc};
use ipld_core::cid::Cid;
use opentelemetry::{global, metrics::Counter};
use reqwest::Client;
use serde::Deserialize;
use serde_ipld_dagcbor::from_reader;
use sqlx::PgPool;
use std::{collections::HashMap, sync::LazyLock, time::Duration};
use tokio::task::spawn_blocking;
use tracing::{instrument, span, trace, warn, Level, Span};

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

/// Convert downloaded files into a database update
#[instrument(skip_all)]
fn convert_repo_to_update(
    repo: Vec<u8>,
    did: &str,
    retrieval_time: DateTime<Utc>,
) -> anyhow::Result<BigUpdate> {
    // Deserialize CAR file
    let (entries, _) = rs_car_sync::car_read_all(&mut repo.as_slice(), true)?;

    // Store the entries in a hashmap for easier access
    let files = entries
        .into_iter()
        .try_fold(HashMap::new(), |mut files, (cid, data)| {
            let cid = Cid::read_bytes(cid.to_bytes().as_slice()).unwrap();
            files.insert(cid, data);
            anyhow::Result::<HashMap<Cid, Vec<u8>>>::Ok(files)
        })?;

    // Create references to the files and the did, so we can use them in the closure
    let files_ref = &files;
    let did_key = &crate::database::utils::did_to_key(did)?;

    let mut update = files_ref
        .iter()
        // Convert to NodeData
        .filter_map(|(_, data)| from_reader::<NodeData, _>(&data[..]).ok())
        // Convert to Updates
        .flat_map(|node_data| {
            // TODO: Understand this logic
            let mut key = "".to_string();
            node_data.entries.into_iter().filter_map(move |entry| {
                let k = match String::from_utf8(entry.key_suffix) {
                    Ok(k) => k,
                    Err(e) => return Some(Err(anyhow::Error::from(e))),
                };
                key = format!("{}{}", key.split_at(entry.prefix_len as usize).0, k);

                let block = files_ref.get(&entry.value)?;
                let record = from_reader::<KnownRecord, _>(&block[..]).ok()?;
                let mut parts = key.split("/");

                let collection = parts.next()?.to_string();
                let rkey = RecordKey::new(parts.next()?.to_string()).ok()?;
                let update = create_big_update(
                    Did::new(did.to_string()).unwrap(),
                    did_key.clone(),
                    collection,
                    rkey,
                    record,
                );
                Some(update)
            })
        })
        // Merge the updates
        .try_fold(BigUpdate::default(), |mut acc, update| {
            acc.merge(update?);
            anyhow::Result::<BigUpdate>::Ok(acc)
        })?;

    // Add the timestamp of when we retrieved the repo to the update
    update.add_timestamp(did, retrieval_time);

    Ok(update)
}

#[derive(Debug)]
pub struct CommonState {
    database: PgPool,
    http_client: Client,
    did: String,
    span: Span,
}

/// First pipeline stage
#[derive(Debug)]
pub struct DownloadService {
    common: CommonState,
}
/// Second pipeline stage
#[derive(Debug)]
pub struct DownloadRepo {
    common: CommonState,
    service: PlcDirectoryDidResponseService,
}
/// Third pipeline stage
#[derive(Debug)]
pub struct ProcessRepo {
    common: CommonState,
    repo: Vec<u8>,
    retrieval_time: DateTime<Utc>,
}
/// Fourth pipeline stage
#[derive(Debug)]
pub struct ApplyUpdates {
    common: CommonState,
    update: BigUpdate,
}

impl DownloadService {
    pub fn new(database: PgPool, http_client: Client, did: String) -> DownloadService {
        let span = span!(target: "backfill", parent: None, Level::INFO, "pipeline_item");
        span.record("did", did.clone());
        span.in_scope(|| {
            trace!("Start backfilling repo");
        });
        DownloadService {
            common: CommonState {
                database,
                http_client,
                did,
                span,
            },
        }
    }
}

impl Stage for DownloadService {
    type Next = DownloadRepo;
    const NAME: &str = "download_information";

    #[instrument(skip(self), fields(did = self.common.did), parent = self.common.span.clone())]
    async fn run(self) -> anyhow::Result<Self::Next> {
        let resp = self
            .common
            .http_client
            .get(format!("https://plc.directory/{}", self.common.did))
            .timeout(Duration::from_secs(ARGS.directory_download_timeout))
            .send()
            .await?
            .json::<PlcDirectoryDidResponse>()
            .await?;
        let service = resp.service.into_iter().next().ok_or(anyhow::anyhow!(
            "Failed to get a plc service for {}",
            self.common.did
        ))?;
        Ok(DownloadRepo {
            service,
            common: self.common,
        })
    }
}

static DOWNLOAD_REPO_RETRIES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("indexer")
        .u64_counter("indexer.pipeline.download_repo_retries")
        .with_unit("{retry}")
        .with_description("Number of retries for downloading a repo")
        .build()
});

async fn attempt_download(
    client: &Client,
    url: &str,
    timeout: Duration,
) -> anyhow::Result<Vec<u8>> {
    let get_repo_response = client.get(url).timeout(timeout).send().await?;
    if !get_repo_response.status().is_success() {
        return Err(anyhow::anyhow!("Statuscode {}", get_repo_response.status()));
    }
    let repo: Vec<u8> = get_repo_response.bytes().await?.into();
    if repo.is_empty() {
        return Err(anyhow::anyhow!("Downloaded repo is empty"));
    }
    Ok(repo)
}

impl Stage for DownloadRepo {
    type Next = ProcessRepo;
    const NAME: &str = "download_repo";

    #[instrument(skip(self), fields(did = self.common.did), parent = self.common.span.clone())]
    async fn run(self) -> anyhow::Result<Self::Next> {
        let retrival_time = chrono::Utc::now();

        // Download the repo
        let mut attempts_left = ARGS.download_repo_attempts;
        let repo = loop {
            let get_repo_response = attempt_download(
                &self.common.http_client,
                &format!(
                    "{}/xrpc/com.atproto.sync.getRepo?did={}",
                    self.service.service_endpoint, self.common.did,
                ),
                Duration::from_secs(ARGS.download_repo_timeout),
            )
            .await;

            let error = match get_repo_response {
                Ok(resp) => {
                    break Ok(resp);
                }
                Err(error) => error,
            };

            attempts_left -= 1;
            trace!(
                "Failed to download repo {} with error: {}, Retrying {} more times",
                self.common.did,
                error,
                attempts_left
            );
            DOWNLOAD_REPO_RETRIES.add(1, &[]);
            if attempts_left == 0 {
                break Err(anyhow::anyhow!(
                    "Failed to download repo {} after {} attempts",
                    self.common.did,
                    ARGS.download_repo_attempts
                ));
            }
        }?;

        trace!(
            "Downloaded repo {} with size {:.2} MB",
            self.common.did,
            repo.len() as f64 / (1000.0 * 1000.0)
        );
        Ok(ProcessRepo {
            repo,
            common: self.common,
            retrieval_time: retrival_time,
        })
    }
}

impl Stage for ProcessRepo {
    type Next = ApplyUpdates;
    const NAME: &str = "process_repo";

    #[instrument(skip(self), fields(did = self.common.did), parent = self.common.span.clone())]
    async fn run(self) -> anyhow::Result<Self::Next> {
        let did = self.common.did.clone();
        let big_update =
            spawn_blocking(move || convert_repo_to_update(self.repo, &did, self.retrieval_time))
                .await??;

        Ok(ApplyUpdates {
            update: big_update,
            common: self.common,
        })
    }
}

impl Stage for ApplyUpdates {
    type Next = NoNextStage;
    const NAME: &str = "apply_updates";

    #[instrument(skip(self), fields(did = self.common.did), parent = self.common.span.clone())]
    async fn run(self) -> anyhow::Result<Self::Next> {
        if !ARGS.no_write_when_backfilling {
            self.update
                .apply(self.common.database.clone(), "backfill")
                .await?;
        } else {
            warn!("Skipping writing to the database and sleeping instead");
            std::thread::sleep(Duration::from_secs(2));
        }
        Ok(NoNextStage {})
    }
}
