use opentelemetry::metrics::Counter;
use opentelemetry::{global, KeyValue};
use std::sync::LazyLock;

use super::BigUpdate;

static INSERTED_ROWS_METRIC: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("indexer")
        .u64_counter("indexer.database.inserted_elements")
        .with_unit("{row}")
        .with_description("Inserted or updated rows")
        .build()
});
static INSERTED_SIZE_METRIC: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("indexer")
        .u64_counter("indexer.database.inserted_bytes")
        .with_unit("By")
        .with_description("Inserted or updated bytes (approximation)")
        .build()
});
static TRANSACTIONS_METRIC: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("indexer")
        .u64_counter("indexer.database.transactions")
        .with_unit("{transaction}")
        .with_description("Number of transactions")
        .build()
});

pub(super) struct BigUpdateInfoRow {
    pub(super) count: u64,
    pub(super) size: u64,
}
impl core::fmt::Debug for BigUpdateInfoRow {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_map()
            .entry(&"count", &self.count)
            .entry(&"mB", &(self.size as f64 / 1024.0 / 1024.0))
            .finish()
    }
}

pub(super) struct BigUpdateInfo {
    // Info about individual tables
    pub(super) did: BigUpdateInfoRow,
    pub(super) follows: BigUpdateInfoRow,
    pub(super) latest_backfills: BigUpdateInfoRow,
    pub(super) likes: BigUpdateInfoRow,
    pub(super) reposts: BigUpdateInfoRow,
    pub(super) blocks: BigUpdateInfoRow,
    pub(super) listblocks: BigUpdateInfoRow,
    pub(super) listitems: BigUpdateInfoRow,
    pub(super) feeds: BigUpdateInfoRow,
    pub(super) lists: BigUpdateInfoRow,
    pub(super) threadgates: BigUpdateInfoRow,
    pub(super) starterpacks: BigUpdateInfoRow,
    pub(super) postgates: BigUpdateInfoRow,
    pub(super) actordeclarations: BigUpdateInfoRow,
    pub(super) labelerservices: BigUpdateInfoRow,
    pub(super) quotes: BigUpdateInfoRow,
    pub(super) posts: BigUpdateInfoRow,
    pub(super) replies_relations: BigUpdateInfoRow,
    pub(super) reply_to_relations: BigUpdateInfoRow,
    pub(super) posts_relations: BigUpdateInfoRow,
    pub(super) overwrite_latest_backfills: BigUpdateInfoRow,
}

impl BigUpdateInfo {
    pub fn new(update: &BigUpdate) -> Self {
        BigUpdateInfo {
            did: BigUpdateInfoRow {
                count: update.did.len() as u64,
                size: update
                    .did
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            follows: BigUpdateInfoRow {
                count: update.follows.len() as u64,
                size: update
                    .follows
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            latest_backfills: BigUpdateInfoRow {
                count: update.latest_backfills.len() as u64,
                size: update
                    .latest_backfills
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            likes: BigUpdateInfoRow {
                count: update.likes.len() as u64,
                size: update
                    .likes
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            reposts: BigUpdateInfoRow {
                count: update.reposts.len() as u64,
                size: update
                    .reposts
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            blocks: BigUpdateInfoRow {
                count: update.blocks.len() as u64,
                size: update
                    .blocks
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            listblocks: BigUpdateInfoRow {
                count: update.listblocks.len() as u64,
                size: update
                    .listblocks
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            listitems: BigUpdateInfoRow {
                count: update.listitems.len() as u64,
                size: update
                    .listitems
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            feeds: BigUpdateInfoRow {
                count: update.feeds.len() as u64,
                size: update
                    .feeds
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            lists: BigUpdateInfoRow {
                count: update.lists.len() as u64,
                size: update
                    .lists
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            threadgates: BigUpdateInfoRow {
                count: update.threadgates.len() as u64,
                size: update
                    .threadgates
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            starterpacks: BigUpdateInfoRow {
                count: update.starterpacks.len() as u64,
                size: update
                    .starterpacks
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            postgates: BigUpdateInfoRow {
                count: update.postgates.len() as u64,
                size: update
                    .postgates
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            actordeclarations: BigUpdateInfoRow {
                count: update.actordeclarations.len() as u64,
                size: update
                    .actordeclarations
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            labelerservices: BigUpdateInfoRow {
                count: update.labelerservices.len() as u64,
                size: update
                    .labelerservices
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            quotes: BigUpdateInfoRow {
                count: update.quotes.len() as u64,
                size: update
                    .quotes
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            posts: BigUpdateInfoRow {
                count: update.posts.len() as u64,
                size: update
                    .posts
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            replies_relations: BigUpdateInfoRow {
                count: update.replies_relations.len() as u64,
                size: update
                    .replies_relations
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            reply_to_relations: BigUpdateInfoRow {
                count: update.reply_to_relations.len() as u64,
                size: update
                    .reply_to_relations
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            posts_relations: BigUpdateInfoRow {
                count: update.posts_relations.len() as u64,
                size: update
                    .posts_relations
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
            overwrite_latest_backfills: BigUpdateInfoRow {
                count: update.overwrite_latest_backfills.len() as u64,
                size: update
                    .overwrite_latest_backfills
                    .iter()
                    .map(|e| serde_ipld_dagcbor::to_vec(e).unwrap().len() as u64)
                    .sum(),
            },
        }
    }
    pub fn all_relations(&self) -> BigUpdateInfoRow {
        BigUpdateInfoRow {
            count: self.likes.count
                + self.reposts.count
                + self.blocks.count
                + self.listblocks.count
                + self.listitems.count
                + self.replies_relations.count
                + self.reply_to_relations.count
                + self.posts_relations.count
                + self.quotes.count
                + self.follows.count,
            size: self.likes.size
                + self.reposts.size
                + self.blocks.size
                + self.listblocks.size
                + self.listitems.size
                + self.replies_relations.size
                + self.reply_to_relations.size
                + self.posts_relations.size
                + self.quotes.size
                + self.follows.size,
        }
    }
    pub fn all_tables(&self) -> BigUpdateInfoRow {
        BigUpdateInfoRow {
            count: self.did.count
                + self.feeds.count
                + self.lists.count
                + self.threadgates.count
                + self.starterpacks.count
                + self.postgates.count
                + self.actordeclarations.count
                + self.labelerservices.count
                + self.posts.count,
            size: self.did.size
                + self.feeds.size
                + self.lists.size
                + self.threadgates.size
                + self.starterpacks.size
                + self.postgates.size
                + self.actordeclarations.size
                + self.labelerservices.size
                + self.posts.size,
        }
    }
    pub fn all(&self) -> BigUpdateInfoRow {
        BigUpdateInfoRow {
            count: self.all_relations().count + self.all_tables().count,
            size: self.all_relations().size + self.all_tables().size,
        }
    }

    pub fn record_metrics(&self, source: &str) {
        INSERTED_ROWS_METRIC.add(
            self.all().count,
            &[KeyValue::new("source", source.to_string())],
        );
        INSERTED_SIZE_METRIC.add(
            self.all().size,
            &[KeyValue::new("source", source.to_string())],
        );
        TRANSACTIONS_METRIC.add(1, &[]);
    }
}

impl core::fmt::Debug for BigUpdateInfo {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_map()
            .entry(&"did", &self.did)
            .entry(&"follows", &self.follows)
            .entry(&"latest_backfills", &self.latest_backfills)
            .entry(&"likes", &self.likes)
            .entry(&"reposts", &self.reposts)
            .entry(&"blocks", &self.blocks)
            .entry(&"listblocks", &self.listblocks)
            .entry(&"listitems", &self.listitems)
            .entry(&"feeds", &self.feeds)
            .entry(&"lists", &self.lists)
            .entry(&"threadgates", &self.threadgates)
            .entry(&"starterpacks", &self.starterpacks)
            .entry(&"postgates", &self.postgates)
            .entry(&"actordeclarations", &self.actordeclarations)
            .entry(&"labelerservices", &self.labelerservices)
            .entry(&"quotes", &self.quotes)
            .entry(&"posts", &self.posts)
            .entry(&"replies_relations", &self.replies_relations)
            .entry(&"reply_to_relations", &self.reply_to_relations)
            .entry(&"posts_relations", &self.posts_relations)
            .entry(
                &"overwrite_latest_backfills",
                &self.overwrite_latest_backfills,
            )
            .finish()
    }
}
