use std::{
    collections::{HashSet, VecDeque},
    future::{Future, IntoFuture},
    task::Poll,
};

use futures::Stream;
use serde::Deserialize;
use surrealdb::{engine::any::Any, Surreal};

use crate::database::utils::unsafe_user_key_to_did;

pub struct RepoStream {
    buffer: VecDeque<String>,
    processed_dids: HashSet<String>,
    db: Surreal<Any>,
    db_future: Option<
        std::pin::Pin<
            Box<dyn Future<Output = Result<surrealdb::Response, surrealdb::Error>> + Send>,
        >,
    >,
}

#[derive(Deserialize)]
struct LatestBackfill {
    pub at: Option<surrealdb::sql::Datetime>,
    pub of: surrealdb::RecordId,
}

impl RepoStream {
    pub fn new(db: Surreal<Any>) -> Self {
        return Self {
            buffer: VecDeque::new(),
            processed_dids: HashSet::new(),
            db,
            db_future: None,
        };
    }
}

const FETCH_AMOUNT: usize = 10000;

// async fn get_repos_from(db: &Surreal<Any>, anchor: &str) -> Vec<String> {
//     info!(target: "indexer", "Discovering follows starting from {}", anchor);
//     let mut result = db
//         // TODO: Fix the possible SQL injection
//         .query(format!(
//             "SELECT id,in,out FROM follow:{}.. LIMIT {};",
//             anchor, FETCH_AMOUNT
//         ));
//     let follows: Vec<BskyFollowRes> = result.take(0)?;

//     let Some(anchor_key) = follows.last().map(|follow| follow.id.key()) else {
//         sleep(DISCOVERY_CAUGHT_UP_BACKOFF).await;
//         continue;
//     };
// }

impl Stream for RepoStream {
    type Item = String;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            if let Some(next) = self.buffer.pop_front() {
                return Poll::Ready(Some(next));
            }
            eprintln!("RepoStream not ready, fetching more data");

            if self.db_future.is_none() {
                self.db_future = Some(
                    self.db
                        // TODO: Fix the possible SQL injection
                        .query(r#"SELECT of FROM latest_backfill WHERE at IS NONE LIMIT $limit;"#)
                        .bind(("limit", FETCH_AMOUNT))
                        .into_owned()
                        .into_future(),
                );
            }
            let db_future = self.db_future.as_mut().unwrap();

            let Poll::Ready(result) = Future::poll(db_future.as_mut(), cx) else {
                return Poll::Pending;
            };
            self.db_future = None;

            let mut result = result.unwrap();

            let follows: Vec<LatestBackfill> = result.take(0).unwrap();

            // let Some(anchor_key) = follows.last().map(|follow| follow.id.key()) else {
            //     // TODO: Sleep again
            //     return Poll::Pending;
            // };
            // self.anchor = format!("{}", anchor_key);

            let starttime = std::time::Instant::now();
            for latest_backfill in &follows {
                let key = latest_backfill.of.key().to_string();
                if self.processed_dids.contains(&key) {
                    continue;
                }
                self.processed_dids.insert(key);
                // TODO: Investigate if we can just use the RecordId directly
                let did = unsafe_user_key_to_did(&format!("{}", latest_backfill.of.key()));
                self.buffer.push_back(did);
            }
            let duration = starttime.elapsed();
            eprintln!(
                "RepoStream processed {} records in {}ms",
                follows.len(),
                duration.as_millis()
            );

            if let Some(next) = self.buffer.pop_front() {
                return Poll::Ready(Some(next));
            }
            return Poll::Pending;
        }
    }
}
