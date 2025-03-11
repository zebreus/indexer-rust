use super::utils::{self, at_uri_to_record_id, blob_ref_to_record_id, did_to_key};
use crate::config::ARGS;
use anyhow::Result;
use atrium_api::app::bsky::richtext::facet::MainFeaturesItem;
use atrium_api::types::Object;
use atrium_api::{
    app::bsky::embed::video,
    record::KnownRecord,
    types::{
        string::{Did, RecordKey},
        Blob, BlobRef,
    },
};
use chrono::{DateTime, Utc};
use futures::lock::Mutex;
use info::BigUpdateInfo;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Gauge, Histogram};
use queries::{
    insert_blocks, insert_feeds, insert_follows, insert_latest_backfills, insert_likes,
    insert_listblocks, insert_listitems, insert_lists, insert_posts, insert_posts_relations,
    insert_profiles, insert_quotes_relations, insert_replies_relations, insert_reply_to_relations,
    insert_reposts, upsert_latest_backfills,
};
use serde::Serialize;
use sqlx::sqlite::any;
use sqlx::PgPool;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::LazyLock;
use std::time::Instant;
use surrealdb::RecordId;
use tokio::sync::Semaphore;
use tracing::{instrument, trace, warn};
use types::{
    BskyBlock, BskyDid, BskyFeed, BskyFollow, BskyLatestBackfill, BskyLike, BskyList,
    BskyListBlock, BskyListItem, BskyPost, BskyPostImage, BskyPostMediaAspectRatio, BskyPostVideo,
    BskyPostVideoBlob, BskyPostsRelation, BskyQuote, BskyRepliesRelation, BskyReplyToRelation,
    BskyRepost, WithId,
};

mod info;
mod queries;
mod types;

static QUERY_DURATION_METRIC: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    global::meter("indexer")
        .u64_histogram("indexer.database.insert_duration")
        .with_unit("ms")
        .with_description("Big update duration")
        .with_boundaries(vec![
            0.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 2500.0, 5000.0,
            7500.0, 10000.0, 25000.0, 50000.0, 75000.0, 100000.0, 250000.0, 500000.0, 750000.0,
            1000000.0, 2500000.0,
        ])
        .build()
});
static NEWLY_DISCOVERED_DIDS_METRIC: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("indexer")
        .u64_counter("indexer.database.newly_discovered_dids")
        .with_unit("{DID}")
        .with_description("Number of newly discovered DIDs")
        .build()
});
static FAILED_BIG_UPDATES_METRIC: LazyLock<Counter<u64>> = LazyLock::new(|| {
    global::meter("indexer")
        .u64_counter("indexer.database.failed_big_updates")
        .with_unit("{update}")
        .with_description("Number of failed big updates. Should be always 0")
        .build()
});
static TRANSACTION_TICKETS_COST_METRIC: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    global::meter("indexer")
        .u64_gauge("indexer.database.transaction_cost")
        .with_unit("{cost}")
        .with_description("The current cost of holding a database transaction")
        .build()
});
static TRANSACTION_TICKETS_AVAILABLE_METRIC: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    global::meter("indexer")
        .u64_gauge("indexer.database.transaction_cost")
        .with_unit("{cost}")
        .with_description("The current cost of holding a database transaction")
        .build()
});
static COLLECTED_UPDATE_SIZE_METRIC: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    global::meter("indexer")
        .u64_gauge("indexer.database.collected_update_size")
        .with_unit("{elements}")
        .with_description("The current cost of holding a database transaction")
        .build()
});

#[derive(Debug, Clone)]
enum UpdateState {
    /// Update was applied
    Applied,
    /// Update was not applied, retry later
    Retry,
}

// Accumulates small updates until a big update is triggered
static SMALL_UPDATE_ACCUMULATOR: LazyLock<Mutex<(usize, BigUpdate)>> =
    LazyLock::new(|| Mutex::new((0, BigUpdate::default())));

#[derive(Default, Clone, Serialize)]
pub struct BigUpdate {
    /// Insert into did
    did: Vec<WithId<BskyDid>>,
    follows: Vec<WithId<BskyFollow>>,
    latest_backfills: Vec<WithId<BskyLatestBackfill>>,
    /// Like latest_backfills but overwrites existing records
    overwrite_latest_backfills: Vec<WithId<BskyLatestBackfill>>,
    likes: Vec<WithId<BskyLike>>,
    reposts: Vec<WithId<BskyRepost>>,
    blocks: Vec<WithId<BskyBlock>>,
    listblocks: Vec<WithId<BskyListBlock>>,
    listitems: Vec<WithId<BskyListItem>>,
    feeds: Vec<WithId<BskyFeed>>,
    lists: Vec<WithId<BskyList>>,
    threadgates: Vec<WithId<Box<Object<atrium_api::app::bsky::feed::threadgate::RecordData>>>>,
    starterpacks: Vec<WithId<Box<Object<atrium_api::app::bsky::graph::starterpack::RecordData>>>>,
    postgates: Vec<WithId<Box<Object<atrium_api::app::bsky::feed::postgate::RecordData>>>>,
    actordeclarations:
        Vec<WithId<Box<Object<atrium_api::chat::bsky::actor::declaration::RecordData>>>>,
    labelerservices: Vec<WithId<Box<Object<atrium_api::app::bsky::labeler::service::RecordData>>>>,
    quotes: Vec<WithId<BskyQuote>>,
    posts: Vec<WithId<BskyPost>>,
    replies_relations: Vec<WithId<BskyRepliesRelation>>,
    reply_to_relations: Vec<WithId<BskyReplyToRelation>>,
    posts_relations: Vec<WithId<BskyPostsRelation>>,
}

// async fn write(
//     writer: BinaryCopyInWriter,
//     data: &Vec<WithId<BskyLatestBackfill>>,
// ) -> Result<usize> {
//     pin_mut!(writer);
//     let mut data = data
//         .iter()
//         .map(|x| {
//             (
//                 x.id.clone(),
//                 x.data.of.key().to_string(),
//                 x.data.at.unwrap_or_default().naive_utc(),
//             )
//         })
//         .collect::<Vec<_>>();
//     let mut row: Vec<&'_ (dyn ToSql + Sync)> = Vec::new();
//     for m in &data {
//         row.clear();
//         row.push(&m.0);
//         row.push(&m.1);
//         row.push(&m.2);
//         writer.as_mut().write(&row).await?;
//     }

//     writer.finish().await?;

//     Ok(data.len())
// }

impl BigUpdate {
    pub fn merge(&mut self, other: BigUpdate) {
        self.did.extend(other.did);
        self.follows.extend(other.follows);
        self.latest_backfills.extend(other.latest_backfills);
        self.likes.extend(other.likes);
        self.reposts.extend(other.reposts);
        self.blocks.extend(other.blocks);
        self.listblocks.extend(other.listblocks);
        self.listitems.extend(other.listitems);
        self.feeds.extend(other.feeds);
        self.lists.extend(other.lists);
        self.threadgates.extend(other.threadgates);
        self.starterpacks.extend(other.starterpacks);
        self.postgates.extend(other.postgates);
        self.actordeclarations.extend(other.actordeclarations);
        self.labelerservices.extend(other.labelerservices);
        self.quotes.extend(other.quotes);
        self.posts.extend(other.posts);
        self.replies_relations.extend(other.replies_relations);
        self.reply_to_relations.extend(other.reply_to_relations);
        self.posts_relations.extend(other.posts_relations);
        self.overwrite_latest_backfills
            .extend(other.overwrite_latest_backfills);
    }

    pub fn add_timestamp(&mut self, did: &str, time: DateTime<Utc>) {
        self.overwrite_latest_backfills.push(WithId {
            id: did.to_string(),
            data: BskyLatestBackfill {
                of: RecordId::from(("did", did)),
                at: Some(time),
            },
        });
    }

    // /// Acquire individual locks for each table
    // ///
    // /// Currently unused
    // async fn acquire_locks(&self) -> Vec<SemaphorePermit> {
    //     static PERMITS: LazyLock<usize> = LazyLock::new(|| 1);
    //     static DID_SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(*PERMITS));
    //     static FOLLOWS_SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(*PERMITS));
    //     static LATEST_BACKFILLS_SEMAPHORE: LazyLock<Semaphore> =
    //         LazyLock::new(|| Semaphore::new(*PERMITS));
    //     static LIKES_SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(*PERMITS));
    //     static REPOSTS_SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(*PERMITS));
    //     static BLOCKS_SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(*PERMITS));
    //     static LISTBLOCKS_SEMAPHORE: LazyLock<Semaphore> =
    //         LazyLock::new(|| Semaphore::new(*PERMITS));
    //     static LISTITEMS_SEMAPHORE: LazyLock<Semaphore> =
    //         LazyLock::new(|| Semaphore::new(*PERMITS));
    //     static FEEDS_SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(*PERMITS));
    //     static LISTS_SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(*PERMITS));
    //     static THREADGATES_SEMAPHORE: LazyLock<Semaphore> =
    //         LazyLock::new(|| Semaphore::new(*PERMITS));
    //     static STARTERPACKS_SEMAPHORE: LazyLock<Semaphore> =
    //         LazyLock::new(|| Semaphore::new(*PERMITS));
    //     static POSTGATES_SEMAPHORE: LazyLock<Semaphore> =
    //         LazyLock::new(|| Semaphore::new(*PERMITS));
    //     static ACTORDECLARATIONS_SEMAPHORE: LazyLock<Semaphore> =
    //         LazyLock::new(|| Semaphore::new(*PERMITS));
    //     static LABELERSERVICES_SEMAPHORE: LazyLock<Semaphore> =
    //         LazyLock::new(|| Semaphore::new(*PERMITS));
    //     static QUOTES_SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(*PERMITS));
    //     static POSTS_SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(*PERMITS));
    //     static REPLIES_RELATIONS_SEMAPHORE: LazyLock<Semaphore> =
    //         LazyLock::new(|| Semaphore::new(*PERMITS));
    //     static REPLY_TO_RELATIONS_SEMAPHORE: LazyLock<Semaphore> =
    //         LazyLock::new(|| Semaphore::new(*PERMITS));
    //     static POSTS_RELATIONS_SEMAPHORE: LazyLock<Semaphore> =
    //         LazyLock::new(|| Semaphore::new(*PERMITS));
    //     static OVERWRITE_LATEST_BACKFILLS_SEMAPHORE: LazyLock<Semaphore> =
    //         LazyLock::new(|| Semaphore::new(*PERMITS));

    //     let mut permits = Vec::new();

    //     if !self.did.is_empty() {
    //         permits.push(DID_SEMAPHORE.acquire().await.unwrap());
    //     }
    //     if !self.follows.is_empty() {
    //         permits.push(FOLLOWS_SEMAPHORE.acquire().await.unwrap());
    //     }
    //     if !self.latest_backfills.is_empty() {
    //         permits.push(LATEST_BACKFILLS_SEMAPHORE.acquire().await.unwrap());
    //     }
    //     if !self.likes.is_empty() {
    //         permits.push(LIKES_SEMAPHORE.acquire().await.unwrap());
    //     }
    //     if !self.reposts.is_empty() {
    //         permits.push(REPOSTS_SEMAPHORE.acquire().await.unwrap());
    //     }
    //     if !self.blocks.is_empty() {
    //         permits.push(BLOCKS_SEMAPHORE.acquire().await.unwrap());
    //     }
    //     if !self.listblocks.is_empty() {
    //         permits.push(LISTBLOCKS_SEMAPHORE.acquire().await.unwrap());
    //     }
    //     if !self.listitems.is_empty() {
    //         permits.push(LISTITEMS_SEMAPHORE.acquire().await.unwrap());
    //     }
    //     if !self.feeds.is_empty() {
    //         permits.push(FEEDS_SEMAPHORE.acquire().await.unwrap());
    //     }
    //     if !self.lists.is_empty() {
    //         permits.push(LISTS_SEMAPHORE.acquire().await.unwrap());
    //     }
    //     if !self.threadgates.is_empty() {
    //         permits.push(THREADGATES_SEMAPHORE.acquire().await.unwrap());
    //     }
    //     if !self.starterpacks.is_empty() {
    //         permits.push(STARTERPACKS_SEMAPHORE.acquire().await.unwrap());
    //     }
    //     if !self.postgates.is_empty() {
    //         permits.push(POSTGATES_SEMAPHORE.acquire().await.unwrap());
    //     }
    //     if !self.actordeclarations.is_empty() {
    //         permits.push(ACTORDECLARATIONS_SEMAPHORE.acquire().await.unwrap());
    //     }
    //     if !self.labelerservices.is_empty() {
    //         permits.push(LABELERSERVICES_SEMAPHORE.acquire().await.unwrap());
    //     }
    //     if !self.quotes.is_empty() {
    //         permits.push(QUOTES_SEMAPHORE.acquire().await.unwrap());
    //     }
    //     if !self.posts.is_empty() {
    //         permits.push(POSTS_SEMAPHORE.acquire().await.unwrap());
    //     }
    //     if !self.replies_relations.is_empty() {
    //         permits.push(REPLIES_RELATIONS_SEMAPHORE.acquire().await.unwrap());
    //     }
    //     if !self.reply_to_relations.is_empty() {
    //         permits.push(REPLY_TO_RELATIONS_SEMAPHORE.acquire().await.unwrap());
    //     }
    //     if !self.posts_relations.is_empty() {
    //         permits.push(POSTS_RELATIONS_SEMAPHORE.acquire().await.unwrap());
    //     }
    //     if !self.overwrite_latest_backfills.is_empty() {
    //         permits.push(
    //             OVERWRITE_LATEST_BACKFILLS_SEMAPHORE
    //                 .acquire()
    //                 .await
    //                 .unwrap(),
    //         );
    //     }

    //     permits
    // }

    async fn actually_attempt_apply(self, database: PgPool) -> Result<()> {
        let BigUpdate {
            did,
            follows,
            latest_backfills,
            likes,
            reposts,
            blocks,
            listblocks,
            listitems,
            feeds,
            lists,
            threadgates,
            starterpacks,
            postgates,
            actordeclarations,
            labelerservices,
            quotes,
            posts,
            replies_relations,
            reply_to_relations,
            posts_relations,
            overwrite_latest_backfills,
        } = self;

        let mut transaction = database.begin().await.unwrap();

        // sqlx::query!("SET LOCAL synchronous_commit = 'off'")
        //     .execute(&mut *transaction)
        //     .await
        //     .unwrap();
        sqlx::query!("SET LOCAL commit_delay = 10000")
            .execute(&mut *transaction)
            .await?;

        sqlx::query!("SET CONSTRAINTS ALL DEFERRED")
            .execute(&mut *transaction)
            .await?;

        insert_profiles(&did, &mut transaction).await?;
        insert_follows(&follows, &mut transaction).await?;
        insert_likes(&likes, &mut transaction).await?;
        insert_reposts(&reposts, &mut transaction).await?;
        insert_blocks(&blocks, &mut transaction).await?;
        insert_listblocks(&listblocks, &mut transaction).await?;
        insert_listitems(&listitems, &mut transaction).await?;
        insert_feeds(&feeds, &mut transaction).await?;
        insert_lists(&lists, &mut transaction).await?;
        // insert_threadgates(&threadgates, &mut transaction).await?;
        // insert_starterpacks(&starterpacks, &mut transaction).await?;
        // insert_postgates(&postgates, &mut transaction).await?;
        // insert_actordeclarations(&actordeclarations, &mut transaction).await?;
        // insert_labelerservices(&labelerservices, &mut transaction).await?;
        insert_quotes_relations(&quotes, &mut transaction).await?;
        insert_replies_relations(&replies_relations, &mut transaction).await?;
        insert_reply_to_relations(&reply_to_relations, &mut transaction).await?;
        insert_posts(&posts, &mut transaction).await?;
        insert_posts_relations(&posts_relations, &mut transaction).await?;
        sqlx::query!("LOCK latest_backfill")
            .execute(&mut *transaction)
            .await?;
        insert_latest_backfills(&latest_backfills, &mut transaction).await?;
        upsert_latest_backfills(&overwrite_latest_backfills, &mut transaction).await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Apply this update to the database
    ///
    /// `source` is a string describing the source of the update, used for metrics
    ///
    /// Apply attempt with a convoluted mechanism to avoid congestion
    async fn attempt_apply(
        &mut self,
        database: PgPool,
        source: &str,
        info: &BigUpdateInfo,
    ) -> Result<UpdateState> {
        let start = Instant::now();

        let after_update = Instant::now();

        // // What follows is a complex mechanism to limit the number of concurrent transactions. We need to do this ourselves because surrealdb just drops conflicting transactions.
        // // The mechanism is as follows:

        // // We have a given budget of permits that can be used for transactions. Each transaction costs a certain amount of permits, the bigger the transaction, the more permits it costs.
        // //
        // // The base cost of a transaction is increased, when transactions are dropped due to congestion, and decreased when transactions are successful.

        // Minimum cost for a transaction in permits
        static MIN_COST: u32 = 20;
        // Maximum cost for a transaction in permits
        static MAX_COST: LazyLock<u32> =
            LazyLock::new(|| MIN_COST * ARGS.max_concurrent_transactions);
        // Semaphore for limiting the number of concurrent transactions by permits
        static SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| {
            Semaphore::new(*MAX_COST as usize * ARGS.min_concurrent_transactions as usize)
        });
        // The current cost of a transaction in permits
        static TRANSACTION_COST: AtomicU32 = AtomicU32::new(MIN_COST);

        let base_cost = TRANSACTION_COST.load(Ordering::Relaxed);
        TRANSACTION_TICKETS_COST_METRIC.record(base_cost as u64, &[]);
        TRANSACTION_TICKETS_AVAILABLE_METRIC.record(SEMAPHORE.available_permits() as u64, &[]);
        // A multiplier for transactions that may cause congestion
        let transaction_cost_multiplier = f64::log10(10.0 + info.all().count as f64).floor() as u32;
        let transaction_cost = std::cmp::min(*MAX_COST, base_cost * transaction_cost_multiplier);

        let result: anyhow::Result<()> = {
            let cloned = self.clone();
            let _permit = SEMAPHORE.acquire_many(transaction_cost).await.unwrap();
            tokio::task::spawn(async move { cloned.actually_attempt_apply(database).await })
        }
        .await
        .unwrap();
        // let errors = result.take_errors();
        // Return retry if the transaction can be retried
        if let Err(error) = &result {
            let can_be_retried = format!("{:?}", error).contains("deadlock");
            if can_be_retried {
                // Raise the cost for each retry
                TRANSACTION_COST
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| {
                        Some(std::cmp::min(*MAX_COST, x * 2))
                    })
                    .unwrap();

                trace!("Transaction can be retried");
                return Ok(UpdateState::Retry);
            }
        }

        // Lower the cost for each successful transaction
        TRANSACTION_COST
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| {
                Some(std::cmp::max(MIN_COST, x - 1))
            })
            .unwrap();
        trace!(
            "Current cost for a transaction is {}",
            TRANSACTION_COST.load(Ordering::Relaxed)
        );

        let update_duration = after_update.elapsed();
        QUERY_DURATION_METRIC.record(update_duration.as_millis() as u64, &[]);

        // // Return error if there are any errors
        if let Err(error) = result {
            // tracing::error!("Database error!!!!!!!!!!!!!!!!!!!!!! {:?}", &error);
            FAILED_BIG_UPDATES_METRIC.add(1, &[]);
            return Err(error.into());

            // let mut sorted_errors = errors.into_iter().collect::<Vec<_>>();
            // sorted_errors.sort_by(|(a, _), (b, _)| a.cmp(b));
            // for error in &sorted_errors {
            //     warn!("Database error: {:?}", error);
            // }
            // let first_error = &sorted_errors.first().unwrap().1;
            // return Err(anyhow::anyhow!("Database error: {:?}", first_error));
        }

        // At this point, we know that the update was successful

        // Record metrics
        info.record_metrics(source);

        // // Record stats about newly discovered DIDs
        // let newly_discovered_dids = result.take::<Vec<IgnoredAny>>(0).unwrap().len();
        // // warn!("Newly discovered DIDs: {}", newly_discovered_dids);
        // if newly_discovered_dids > 0 {
        //     NEWLY_DISCOVERED_DIDS_METRIC.add(newly_discovered_dids as u64, &[]);
        // }

        trace!(
            "Applied updated: {} elements, {}MB, {:03}ms applying",
            info.all().count,
            info.all().size as f64 / 1024.0 / 1024.0,
            update_duration.as_millis(),
        );
        // debug!("Detailed infos: {:?}", info);

        Ok(UpdateState::Applied)
    }

    // /// Apply this update to the database
    // ///
    // /// `source` is a string describing the source of the update, used for metrics
    // ///
    // /// Apply attempt with a convoluted mechanism to avoid congestion
    // async fn attempt_apply(
    //     &mut self,
    //     db: &Surreal<Any>,
    //     source: &str,
    //     info: &BigUpdateInfo,
    // ) -> Result<UpdateState> {
    //     let start = Instant::now();
    //     // Convert the update to a string for logging later

    //     // Create the query string
    //     // `RETURN VALUE none` is used to get empty return values for counting the number of inserted rows
    //     let query_string = r#"
    //         BEGIN;
    //         INSERT IGNORE INTO latest_backfill $latest_backfills RETURN VALUE none;
    //         INSERT IGNORE INTO did $dids RETURN NONE;
    //         INSERT IGNORE INTO feed $feeds RETURN NONE;
    //         INSERT IGNORE INTO list $lists RETURN NONE;
    //         INSERT IGNORE INTO lex_app_bsky_feed_threadgate $threadgates RETURN NONE;
    //         INSERT IGNORE INTO lex_app_bsky_graph_starterpack $starterpacks RETURN NONE;
    //         INSERT IGNORE INTO lex_app_bsky_feed_postgate $postgates RETURN NONE;
    //         INSERT IGNORE INTO lex_chat_bsky_actor_declaration $actordeclarations RETURN NONE;
    //         INSERT IGNORE INTO lex_app_bsky_labeler_service $labelerservices RETURN NONE;
    //         INSERT IGNORE INTO post $posts RETURN NONE;
    //         INSERT RELATION INTO posts $posts_relations RETURN NONE;
    //         INSERT RELATION INTO quotes $quotes RETURN NONE;
    //         INSERT RELATION INTO like $likes RETURN NONE;
    //         INSERT RELATION INTO repost $reposts RETURN NONE;
    //         INSERT RELATION INTO block $blocks RETURN NONE;
    //         INSERT RELATION INTO listblock $listblocks RETURN NONE;
    //         INSERT RELATION INTO listitem $listitems RETURN NONE;
    //         INSERT RELATION INTO replyto $reply_to_relations RETURN NONE;
    //         INSERT RELATION INTO quotes $quotes RETURN NONE;
    //         INSERT RELATION INTO replies $replies_relations RETURN NONE;
    //         INSERT RELATION INTO follow $follows RETURN NONE;
    //         FOR $backfill in $overwrite_latest_backfill {
    //             UPSERT type::thing("latest_backfill", $backfill.id) MERGE $backfill;
    //         };
    //         COMMIT;
    //     "#;

    //     // Create the update query. Does not take that long; ~50ms for 30000 rows
    //     let update = tokio::task::block_in_place(|| {
    //         db.query(query_string)
    //             .bind(("dids", self.did.clone()))
    //             .bind(("follows", self.follows.clone()))
    //             .bind(("latest_backfills", self.latest_backfills.clone()))
    //             .bind(("likes", self.likes.clone()))
    //             .bind(("reposts", self.reposts.clone()))
    //             .bind(("blocks", self.blocks.clone()))
    //             .bind(("listblocks", self.listblocks.clone()))
    //             .bind(("listitems", self.listitems.clone()))
    //             .bind(("feeds", self.feeds.clone()))
    //             .bind(("lists", self.lists.clone()))
    //             .bind(("threadgates", self.threadgates.clone()))
    //             .bind(("starterpacks", self.starterpacks.clone()))
    //             .bind(("postgates", self.postgates.clone()))
    //             .bind(("actordeclarations", self.actordeclarations.clone()))
    //             .bind(("labelerservices", self.labelerservices.clone()))
    //             .bind(("quotes", self.quotes.clone()))
    //             .bind(("posts", self.posts.clone()))
    //             .bind(("replies_relations", self.replies_relations.clone()))
    //             .bind(("reply_to_relations", self.reply_to_relations.clone()))
    //             .bind(("posts_relations", self.posts_relations.clone()))
    //             .bind((
    //                 "overwrite_latest_backfill",
    //                 self.overwrite_latest_backfills.clone(),
    //             ))
    //             .into_future()
    //             .instrument(span!(Level::INFO, "query"))
    //     });

    //     let preparation_duration = start.elapsed();
    //     let after_update = Instant::now();

    //     // Minimum cost for a transaction in permits
    //     static MIN_COST: u32 = 20;
    //     // Maximum cost for a transaction in permits
    //     static MAX_COST: LazyLock<u32> =
    //         LazyLock::new(|| MIN_COST * ARGS.max_concurrent_transactions);
    //     // Semaphore for limiting the number of concurrent transactions by permits
    //     static SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| {
    //         Semaphore::new(*MAX_COST as usize * ARGS.min_concurrent_transactions as usize)
    //     });
    //     // The current cost of a transaction in permits
    //     static TRANSACTION_COST: AtomicU32 = AtomicU32::new(MIN_COST);

    //     let base_cost = TRANSACTION_COST.load(Ordering::Relaxed);
    //     TRANSACTION_TICKETS_COST_METRIC.record(base_cost as u64, &[]);
    //     TRANSACTION_TICKETS_AVAILABLE_METRIC.record(SEMAPHORE.available_permits() as u64, &[]);
    //     // A multiplier for transactions that may cause congestion
    //     let transaction_cost_multiplier = f64::log10(10.0 + info.all().count as f64).floor() as u32;
    //     let transaction_cost = std::cmp::min(*MAX_COST, base_cost * transaction_cost_multiplier);
    //     let mut result = {
    //         let _permit = SEMAPHORE.acquire_many(transaction_cost).await.unwrap();
    //         update.await
    //     }?;
    //     let errors = result.take_errors();

    //     // Return retry if the transaction can be retried
    //     if errors.len() > 0 {
    //         let can_be_retried = errors.iter().any(|(_, e)| {
    //             if let surrealdb::Error::Api(surrealdb::error::Api::Query(message)) = e {
    //                 message.contains("This transaction can be retried")
    //             } else {
    //                 false
    //             }
    //         });

    //         if can_be_retried {
    //             // Raise the cost for each retry
    //             TRANSACTION_COST
    //                 .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| {
    //                     Some(std::cmp::min(*MAX_COST, x * 2))
    //                 })
    //                 .unwrap();

    //             warn!("Failed but can be retried");
    //             return Ok(UpdateState::Retry);
    //         }
    //     }

    //     // Lower the cost for each successful transaction
    //     TRANSACTION_COST
    //         .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| {
    //             Some(std::cmp::max(MIN_COST, x - 1))
    //         })
    //         .unwrap();
    //     warn!("Cost: {}", TRANSACTION_COST.load(Ordering::Relaxed));

    //     let update_duration = after_update.elapsed();
    //     QUERY_DURATION_METRIC.record(update_duration.as_millis() as u64, &[]);

    //     // Return error if there are any errors
    //     if errors.len() > 0 {
    //         FAILED_BIG_UPDATES_METRIC.add(1, &[]);

    //         let mut sorted_errors = errors.into_iter().collect::<Vec<_>>();
    //         sorted_errors.sort_by(|(a, _), (b, _)| a.cmp(b));
    //         for error in &sorted_errors {
    //             warn!("Database error: {:?}", error);
    //         }
    //         let first_error = &sorted_errors.first().unwrap().1;
    //         return Err(anyhow::anyhow!("Database error: {:?}", first_error));
    //     }

    //     // At this point, we know that the update was successful

    //     // Record metrics
    //     info.record_metrics(source);

    //     // Record stats about newly discovered DIDs
    //     let newly_discovered_dids = result.take::<Vec<IgnoredAny>>(0).unwrap().len();
    //     // warn!("Newly discovered DIDs: {}", newly_discovered_dids);
    //     if newly_discovered_dids > 0 {
    //         NEWLY_DISCOVERED_DIDS_METRIC.add(newly_discovered_dids as u64, &[]);
    //     }

    //     trace!(
    //         "Applied updated: {} elements, {}MB, {:03}ms preparation, {:03}ms applying",
    //         info.all().count,
    //         info.all().size as f64 / 1024.0 / 1024.0,
    //         preparation_duration.as_millis(),
    //         update_duration.as_millis(),
    //     );
    //     debug!("Detailed infos: {:?}", info);

    //     Ok(UpdateState::Applied)
    // }

    // /// apply update with individual locks for each table
    // async fn attempt_apply(
    //     &mut self,
    //     db: &Surreal<Any>,
    //     source: &str,
    //     info: &BigUpdateInfo,
    // ) -> Result<UpdateState> {
    //     let start = Instant::now();
    //     // Convert the update to a string for logging later

    //     // Create the query string
    //     // `RETURN VALUE none` is used to get empty return values for counting the number of inserted rows
    //     let query_string = r#"
    //         BEGIN;
    //         INSERT IGNORE INTO latest_backfill $latest_backfills RETURN VALUE none;
    //         INSERT IGNORE INTO did $dids RETURN NONE;
    //         INSERT IGNORE INTO feed $feeds RETURN NONE;
    //         INSERT IGNORE INTO list $lists RETURN NONE;
    //         INSERT IGNORE INTO lex_app_bsky_feed_threadgate $threadgates RETURN NONE;
    //         INSERT IGNORE INTO lex_app_bsky_graph_starterpack $starterpacks RETURN NONE;
    //         INSERT IGNORE INTO lex_app_bsky_feed_postgate $postgates RETURN NONE;
    //         INSERT IGNORE INTO lex_chat_bsky_actor_declaration $actordeclarations RETURN NONE;
    //         INSERT IGNORE INTO lex_app_bsky_labeler_service $labelerservices RETURN NONE;
    //         INSERT IGNORE INTO post $posts RETURN NONE;
    //         INSERT RELATION INTO posts $posts_relations RETURN NONE;
    //         INSERT RELATION INTO quotes $quotes RETURN NONE;
    //         INSERT RELATION INTO like $likes RETURN NONE;
    //         INSERT RELATION INTO repost $reposts RETURN NONE;
    //         INSERT RELATION INTO block $blocks RETURN NONE;
    //         INSERT RELATION INTO listblock $listblocks RETURN NONE;
    //         INSERT RELATION INTO listitem $listitems RETURN NONE;
    //         INSERT RELATION INTO replyto $reply_to_relations RETURN NONE;
    //         INSERT RELATION INTO quotes $quotes RETURN NONE;
    //         INSERT RELATION INTO replies $replies_relations RETURN NONE;
    //         INSERT RELATION INTO follow $follows RETURN NONE;
    //         FOR $backfill in $overwrite_latest_backfill {
    //             UPSERT type::thing("latest_backfill", $backfill.id) MERGE $backfill;
    //         };
    //         COMMIT;
    //     "#;

    //     // Create the update query. Does not take that long; ~50ms for 30000 rows
    //     let update = tokio::task::block_in_place(|| {
    //         db.query(query_string)
    //             .bind(("dids", self.did.clone()))
    //             .bind(("follows", self.follows.clone()))
    //             .bind(("latest_backfills", self.latest_backfills.clone()))
    //             .bind(("likes", self.likes.clone()))
    //             .bind(("reposts", self.reposts.clone()))
    //             .bind(("blocks", self.blocks.clone()))
    //             .bind(("listblocks", self.listblocks.clone()))
    //             .bind(("listitems", self.listitems.clone()))
    //             .bind(("feeds", self.feeds.clone()))
    //             .bind(("lists", self.lists.clone()))
    //             .bind(("threadgates", self.threadgates.clone()))
    //             .bind(("starterpacks", self.starterpacks.clone()))
    //             .bind(("postgates", self.postgates.clone()))
    //             .bind(("actordeclarations", self.actordeclarations.clone()))
    //             .bind(("labelerservices", self.labelerservices.clone()))
    //             .bind(("quotes", self.quotes.clone()))
    //             .bind(("posts", self.posts.clone()))
    //             .bind(("replies_relations", self.replies_relations.clone()))
    //             .bind(("reply_to_relations", self.reply_to_relations.clone()))
    //             .bind(("posts_relations", self.posts_relations.clone()))
    //             .bind((
    //                 "overwrite_latest_backfill",
    //                 self.overwrite_latest_backfills.clone(),
    //             ))
    //             .into_future()
    //             .instrument(span!(Level::INFO, "query"))
    //     });

    //     let preparation_duration = start.elapsed();
    //     let after_update = Instant::now();

    //     let mut result = {
    //         let _permit = self.acquire_locks().await;
    //         update.await
    //     }?;
    //     let errors = result.take_errors();

    //     // Return retry if the transaction can be retried
    //     if errors.len() > 0 {
    //         let can_be_retried = errors.iter().any(|(_, e)| {
    //             if let surrealdb::Error::Api(surrealdb::error::Api::Query(message)) = e {
    //                 message.contains("This transaction can be retried")
    //             } else {
    //                 false
    //             }
    //         });

    //         if can_be_retried {
    //             // Raise the cost for each retry
    //             panic!("Retry not implemented");

    //             warn!("Failed but can be retried");
    //             return Ok(UpdateState::Retry);
    //         }
    //     }

    //     let update_duration = after_update.elapsed();
    //     QUERY_DURATION_METRIC.record(update_duration.as_millis() as u64, &[]);

    //     // Return error if there are any errors
    //     if errors.len() > 0 {
    //         FAILED_BIG_UPDATES_METRIC.add(1, &[]);

    //         let mut sorted_errors = errors.into_iter().collect::<Vec<_>>();
    //         sorted_errors.sort_by(|(a, _), (b, _)| a.cmp(b));
    //         for error in &sorted_errors {
    //             warn!("Database error: {:?}", error);
    //         }
    //         let first_error = &sorted_errors.first().unwrap().1;
    //         return Err(anyhow::anyhow!("Database error: {:?}", first_error));
    //     }

    //     // At this point, we know that the update was successful

    //     // Record metrics
    //     info.record_metrics(source);

    //     // Record stats about newly discovered DIDs
    //     let newly_discovered_dids = result.take::<Vec<IgnoredAny>>(0).unwrap().len();
    //     // warn!("Newly discovered DIDs: {}", newly_discovered_dids);
    //     if newly_discovered_dids > 0 {
    //         NEWLY_DISCOVERED_DIDS_METRIC.add(newly_discovered_dids as u64, &[]);
    //     }

    //     trace!(
    //         "Applied updated: {} elements, {}MB, {:03}ms preparation, {:03}ms applying",
    //         info.all().count,
    //         info.all().size as f64 / 1024.0 / 1024.0,
    //         preparation_duration.as_millis(),
    //         update_duration.as_millis(),
    //     );
    //     debug!("Detailed infos: {:?}", info);

    //     Ok(UpdateState::Applied)
    // }

    /// Apply this update to the database
    ///
    /// `source` is a string describing the source of the update, used for metrics
    pub async fn apply(self, database: PgPool, source: &str) -> Result<()> {
        // If updates are too small, we add them into an accumulator and return here.
        // The accumulated updates will be flushed when it is big enough.
        let (mut update, info) = {
            let info = tokio::task::block_in_place(|| BigUpdateInfo::new(&self));

            let all = info.all();
            if all.count < ARGS.min_rows_per_transaction as u64 {
                // Small update
                let mut lock = SMALL_UPDATE_ACCUMULATOR.lock().await;
                let (count, update) = &mut *lock;
                *count += all.count as usize;
                COLLECTED_UPDATE_SIZE_METRIC.record(*count as u64, &[]);
                update.merge(self);
                if *count < ARGS.min_rows_per_transaction {
                    return Ok(());
                }
                let update = std::mem::take(update);
                *count = 0;
                drop(lock);
                let info = tokio::task::block_in_place(|| BigUpdateInfo::new(&update));

                (update, info)
            } else {
                (self, info)
            }
        };

        // This number is really big, because updates should always succeed after a few retries
        let mut attempts_left = 100;
        loop {
            let state = update
                .attempt_apply(database.clone(), source, &info)
                .await?;
            match state {
                UpdateState::Applied => {
                    break;
                }
                UpdateState::Retry => {
                    trace!("Retrying update {} attempts left", attempts_left);
                    attempts_left -= 1;
                    if attempts_left == 0 {
                        return Err(anyhow::anyhow!(
                            "Failed to apply an update after 100 retries. This needs investigation."
                        ));
                    }
                }
            }
        }
        if attempts_left < 100 {
            trace!("Update successful after {} retries", 100 - attempts_left);
        }

        Ok(())
    }
}

impl core::fmt::Debug for BigUpdate {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let info = BigUpdateInfo::new(self);
        info.fmt(f)
    }
}

/// If the new commit is a create or update, handle it
#[instrument(skip(record))]
pub fn create_big_update(
    did: Did,
    did_key: String,
    collection: String,
    rkey: RecordKey,
    record: KnownRecord,
) -> Result<BigUpdate> {
    utils::ensure_valid_rkey(rkey.to_string())?;

    let mut big_update = BigUpdate::default();

    match record {
        KnownRecord::AppBskyActorProfile(d) => {
            // NOTE: using .ok() here isn't optimal, incorrect data should
            // probably not be entered into the database at all, but for now
            // we'll just ignore it.
            let profile = WithId {
                id: did_key.clone(),
                data: BskyDid {
                    display_name: d.display_name.clone(),
                    description: d.description.clone(),
                    avatar: None, // TODO Implement
                    banner: None, // TODO Implement
                    created_at: d
                        .created_at
                        .as_ref()
                        .and_then(|dt| Some(dt.as_ref().to_utc())),
                    seen_at: Utc::now().into(),
                    joined_via_starter_pack: d
                        .joined_via_starter_pack
                        .as_ref()
                        .and_then(|d| utils::strong_ref_to_record_id(d).ok()),
                    // TODO if strong_ref_to_record_id fails, it should return an error result instead of being empty
                    pinned_post: d
                        .pinned_post
                        .as_ref()
                        .and_then(|d| utils::strong_ref_to_record_id(d).ok()),
                    labels: d
                        .labels
                        .as_ref()
                        .map(utils::extract_self_labels_profile)
                        .unwrap_or_default(),
                    extra_data: process_extra_data(&d.extra_data)?,
                },
            };
            big_update.did.push(profile);
        }
        KnownRecord::AppBskyGraphFollow(d) => {
            // TODO ensure_valid_rkey_strict(rkey.as_str())?;
            let from = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), from);
            let to = utils::did_to_key(d.subject.as_str())?;
            let created_at = d.created_at.as_ref().to_utc();

            big_update.follows.push(WithId {
                id,
                data: BskyFollow {
                    from: RecordId::from(("did", from)),
                    to: RecordId::from(("did", to.clone())),
                    created_at,
                },
            });

            big_update.latest_backfills.push(WithId {
                id: to.clone(),
                data: BskyLatestBackfill {
                    of: RecordId::from(("did", to)),
                    at: None,
                },
            });
        }
        KnownRecord::AppBskyFeedLike(d) => {
            // TODO ensure_valid_rkey_strict(rkey.as_str())?;
            let from = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), from);
            let to = utils::at_uri_to_record_id(&d.subject.uri)?;
            let created_at = d.created_at.as_ref().to_utc();

            big_update.likes.push(WithId {
                id,
                data: BskyLike {
                    from: RecordId::from(("did", from)),
                    to,
                    created_at,
                },
            });
        }
        KnownRecord::AppBskyFeedRepost(d) => {
            // TODO ensure_valid_rkey_strict(rkey.as_str())?;
            let from = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), from);
            let to = utils::at_uri_to_record_id(&d.subject.uri)?;
            let created_at = d.created_at.as_ref().to_utc();

            big_update.reposts.push(WithId {
                id,
                data: BskyRepost {
                    from: RecordId::from(("did", from)),
                    to,
                    created_at,
                },
            });
        }
        KnownRecord::AppBskyGraphBlock(d) => {
            // TODO ensure_valid_rkey_strict(rkey.as_str())?;
            let from = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), from);
            let to = utils::did_to_key(d.subject.as_str())?;
            let created_at = d.created_at.as_ref().to_utc();

            big_update.blocks.push(WithId {
                id,
                data: BskyBlock {
                    from: RecordId::from(("did", from)),
                    to: RecordId::from(("did", to.clone())),
                    created_at,
                },
            });
        }
        KnownRecord::AppBskyGraphListblock(d) => {
            // TODO ensure_valid_rkey_strict(rkey.as_str())?;
            let from = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), from);
            let to = utils::at_uri_to_record_id(&d.subject)?;
            let created_at = d.created_at.as_ref().to_utc();

            big_update.listblocks.push(WithId {
                id,
                data: BskyListBlock {
                    from: RecordId::from(("did", from)),
                    to,
                    created_at,
                },
            });
        }
        KnownRecord::AppBskyGraphListitem(d) => {
            // TODO ensure_valid_rkey_strict(rkey.as_str())?;
            let from = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), from);

            let from = utils::at_uri_to_record_id(&d.list)?;
            let to = utils::did_to_key(&d.subject)?;
            let created_at = d.created_at.as_ref().to_utc();

            big_update.listitems.push(WithId {
                id,
                data: BskyListItem {
                    from,
                    to: RecordId::from(("did", to.clone())),
                    created_at,
                },
            });
        }
        KnownRecord::AppBskyFeedGenerator(d) => {
            let did_key = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), did_key);
            let feed = WithId {
                id,
                data: BskyFeed {
                    author: RecordId::from_table_key("did", did_key),
                    avatar: None, // TODO implement
                    created_at: d.created_at.as_ref().to_utc(),
                    description: d.description.clone(),
                    did: d.did.to_string(),
                    display_name: d.display_name.clone(),
                    rkey: rkey.to_string(),
                    uri: format!(
                        "at://{}/app.bsky.feed.generator/{}",
                        did.as_str(),
                        rkey.as_str()
                    ),
                    extra_data: process_extra_data(&d.extra_data)?,
                },
            };
            big_update.feeds.push(feed);
        }
        KnownRecord::AppBskyGraphList(d) => {
            let did_key = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), did_key);

            let list = WithId {
                id,
                data: BskyList {
                    name: d.name.clone(),
                    avatar: None, // TODO implement
                    created_at: d.created_at.as_ref().to_utc(),
                    description: d.description.clone(),
                    labels: d.labels.as_ref().and_then(utils::extract_self_labels_list),
                    purpose: d.purpose.clone(),
                    extra_data: process_extra_data(&d.extra_data)?,
                },
            };
            big_update.lists.push(list);
        }
        KnownRecord::AppBskyFeedThreadgate(d) => {
            let did_key = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), did_key);
            big_update.threadgates.push(WithId { id, data: d });
        }
        KnownRecord::AppBskyGraphStarterpack(d) => {
            let did_key = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), did_key);
            big_update.starterpacks.push(WithId { id, data: d });
        }
        KnownRecord::AppBskyFeedPostgate(d) => {
            let did_key = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), did_key);
            big_update.postgates.push(WithId { id, data: d });
        }
        KnownRecord::ChatBskyActorDeclaration(d) => {
            let did_key = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), did_key);
            big_update.actordeclarations.push(WithId { id, data: d });
        }
        KnownRecord::AppBskyLabelerService(d) => {
            let did_key = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), did_key);
            big_update.labelerservices.push(WithId { id, data: d });
        }
        KnownRecord::AppBskyFeedPost(d) => {
            let did_key = utils::did_to_key(did.as_str())?;
            let id = format!("{}_{}", rkey.as_str(), did_key);

            let mut images: Vec<BskyPostImage> = vec![];
            let mut links: Vec<String> = vec![];
            let mut mentions: Vec<RecordId> = vec![];
            let mut record: Option<RecordId> = None;
            let mut tags: Vec<String> = vec![];
            let mut video: Option<BskyPostVideo> = None;

            let mut post_images: Vec<atrium_api::app::bsky::embed::images::Image> = vec![];

            if let Some(d) = &d.embed {
                match d {
                    atrium_api::types::Union::Refs(e) => {
                        match e {
                      atrium_api::app::bsky::feed::post::RecordEmbedRefs::AppBskyEmbedExternalMain(m)=>{
                        // TODO index preview too
                        links.push(m.external.uri.clone());
                      },
                        atrium_api::app::bsky::feed::post::RecordEmbedRefs::AppBskyEmbedImagesMain(m) => {
                          post_images=m.images.clone();
                        },
                        atrium_api::app::bsky::feed::post::RecordEmbedRefs::AppBskyEmbedVideoMain(m) => {
                          video = Some(process_video(m)?);
                        },
                        atrium_api::app::bsky::feed::post::RecordEmbedRefs::AppBskyEmbedRecordMain(m) => {
                          record = Some(at_uri_to_record_id(&m.record.uri)?);
                        },
                        atrium_api::app::bsky::feed::post::RecordEmbedRefs::AppBskyEmbedRecordWithMediaMain(m) => {
                          record = Some(at_uri_to_record_id(&m.record.record.uri)?);

                          match &m.media{
                            atrium_api::types::Union::Refs(r)=>match r{
                              atrium_api::app::bsky::embed::record_with_media::MainMediaRefs::AppBskyEmbedExternalMain(m)=>{
                                // TODO index preview too
                                links.push(m.external.uri.clone());
                              }
                              atrium_api::app::bsky::embed::record_with_media::MainMediaRefs::AppBskyEmbedImagesMain(m)=>{
                                post_images=m.images.clone();
                              }
                              atrium_api::app::bsky::embed::record_with_media::MainMediaRefs::AppBskyEmbedVideoMain(m)=>{

                                video = Some(process_video(m)?);
                              }
                            }
                            atrium_api::types::Union::Unknown(_)=>{}
                          }
                        },
                    }
                    }
                    atrium_api::types::Union::Unknown(_) => {}
                }
            };

            if !post_images.is_empty() {
                for i in post_images {
                    images.push(BskyPostImage {
                        alt: i.alt.clone(),
                        blob: blob_ref_to_record_id(&i.image), // TODO store blob details
                        aspect_ratio: i.aspect_ratio.as_ref().map(|a| BskyPostMediaAspectRatio {
                            height: a.height.into(),
                            width: a.width.into(),
                        }),
                    })
                }
            }

            if let Some(facets) = &d.facets {
                for facet in facets {
                    for feature in &facet.features {
                        match feature {
                            atrium_api::types::Union::Refs(refs) => match refs {
                                MainFeaturesItem::Mention(m) => {
                                    mentions.push(("did", did_to_key(m.did.as_str())?).into());
                                }
                                MainFeaturesItem::Link(l) => {
                                    links.push(l.uri.clone());
                                }
                                MainFeaturesItem::Tag(t) => {
                                    tags.push(t.tag.clone());
                                }
                            },
                            atrium_api::types::Union::Unknown(_) => {}
                        }
                    }
                }
            }

            if let Some(t) = &d.tags {
                tags.extend(t.clone());
            }

            if let Some(r) = &record {
                if r.table() == "post" {
                    big_update.quotes.push(WithId {
                        id: id.clone(),
                        data: BskyQuote {
                            from: RecordId::from_table_key("post", id.clone()),
                            to: r.clone(),
                        },
                    });
                }
            }

            let post = WithId {
                id: id.clone(),
                data: BskyPost {
                    author: RecordId::from_table_key("did", did_key.clone()),
                    bridgy_original_url: None,
                    via: None,
                    created_at: d.created_at.as_ref().to_utc(),
                    labels: d.labels.as_ref().and_then(utils::extract_self_labels_post),
                    text: d.text.clone(),
                    langs: d
                        .langs
                        .as_ref()
                        .map(|d| d.iter().map(|l| l.as_ref().to_string()).collect()),
                    root: d
                        .reply
                        .as_ref()
                        .map(|r| utils::strong_ref_to_record_id(&r.root))
                        .transpose()?,
                    parent: d
                        .reply
                        .as_ref()
                        .map(|r| utils::strong_ref_to_record_id(&r.parent))
                        .transpose()?,
                    video,
                    tags: if tags.is_empty() { None } else { Some(tags) },
                    links: if links.is_empty() { None } else { Some(links) },
                    mentions: if mentions.is_empty() {
                        None
                    } else {
                        Some(mentions)
                    },
                    record,
                    images: if images.is_empty() {
                        None
                    } else {
                        Some(images)
                    },
                    extra_data: process_extra_data(&d.extra_data)?,
                },
            };

            let parent = post.data.parent.clone();
            big_update.posts.push(post);

            if parent.is_some() {
                big_update.replies_relations.push(WithId {
                    id: id.clone(),
                    data: BskyRepliesRelation {
                        from: RecordId::from_table_key("did", did_key.clone()),
                        to: RecordId::from_table_key("post", id.clone()),
                    },
                });

                big_update.reply_to_relations.push(WithId {
                    id: id.clone(),
                    data: BskyReplyToRelation {
                        from: RecordId::from_table_key("post", id.clone()),
                        to: parent.unwrap(),
                    },
                });
            } else {
                big_update.posts_relations.push(WithId {
                    id: id.clone(),
                    data: BskyPostsRelation {
                        from: RecordId::from_table_key("did", did_key.clone()),
                        to: RecordId::from_table_key("post", id.clone()),
                    },
                });
            }
        }
        _ => {
            warn!(target: "indexer", "ignored create_or_update {} {} {}",
                did.as_str(), collection, rkey.as_str());
        }
    }

    Ok(big_update)
}

fn process_video(vid: &video::Main) -> Result<BskyPostVideo> {
    let blob = extract_video_blob(&vid.video)?;
    let v = BskyPostVideo {
        alt: vid.alt.clone(),
        aspect_ratio: vid.aspect_ratio.clone().map(|a| BskyPostMediaAspectRatio {
            height: a.height.into(),
            width: a.width.into(),
        }),
        blob: BskyPostVideoBlob {
            cid: blob.r#ref.0.to_string(),
            media_type: blob.mime_type,
            size: blob.size as u64,
        },
        captions: None, // TODO implement
    };
    Ok(v)
}
fn extract_video_blob(blob: &BlobRef) -> Result<Blob> {
    match blob {
        atrium_api::types::BlobRef::Typed(a) => match a {
            atrium_api::types::TypedBlobRef::Blob(b) => Ok(b.clone()),
        },
        atrium_api::types::BlobRef::Untyped(_) => anyhow::bail!("Invalid blob ref type"),
    }
}

fn process_extra_data(ipld: &ipld_core::ipld::Ipld) -> Result<Option<String>> {
    let str = simd_json::serde::to_string(ipld)?;
    Ok(if str == "{}" { None } else { Some(str) })
}
