use std::{
    collections::{HashSet, VecDeque},
    future::{Future, IntoFuture},
    task::Poll,
};

use futures::Stream;
use surrealdb::{engine::any::Any, Surreal};
use tracing::info;

use crate::database::{repo_indexer::BskyFollowRes, utils::unsafe_user_key_to_did};

pub struct RepoStream<'a> {
    buffer: VecDeque<String>,
    processed_dids: HashSet<String>,
    anchor: String,
    db: &'a Surreal<Any>,
    db_future: Option<
        std::pin::Pin<
            Box<
                dyn Future<Output = Result<surrealdb::Response, surrealdb::Error>>
                    + Send
                    + Sync
                    + 'a,
            >,
        >,
    >,
}

impl<'a> RepoStream<'a> {
    pub fn new(anchor: String, db: &'a Surreal<Any>) -> Self {
        return Self {
            buffer: VecDeque::new(),
            processed_dids: HashSet::new(),
            anchor,
            db,
            db_future: None,
        };
    }
}

const FETCH_AMOUNT: usize = 100;

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

impl<'a> Stream for RepoStream<'a> {
    type Item = String;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            if let Some(next) = self.buffer.pop_front() {
                return Poll::Ready(Some(next));
            }

            info!(target: "indexer", "Discovering follows starting from {}", self.anchor);
            let db_future = if self.db_future.is_some() {
                self.db_future.as_mut().unwrap()
            } else {
                let mut result = self
                    .db
                    // TODO: Fix the possible SQL injection
                    .query(format!(
                        "SELECT id,in,out FROM follow:{}.. LIMIT {};",
                        self.anchor, FETCH_AMOUNT
                    ));
                // let mut future: std::pin::Pin<
                //     Box<
                //         dyn Future<Output = Result<surrealdb::Response, surrealdb::Error>>
                //             + Send
                //             + Sync
                //             + 'a,
                //     >,
                // >
                let mut future = result.into_future();
                self.db_future = Some(future);
                self.db_future.as_mut().unwrap()
            };

            let Poll::Ready(result) = Future::poll(db_future.as_mut(), cx) else {
                return Poll::Pending;
            };
            self.db_future = None;

            let mut result = result.unwrap();

            // let mut result: surrealdb::method::Query<'_, Any> = self
            //     .db
            //     // TODO: Fix the possible SQL injection
            //     .query(format!(
            //         "SELECT id,in,out FROM follow:{}.. LIMIT {};",
            //         self.anchor, FETCH_AMOUNT
            //     ));
            let follows: Vec<BskyFollowRes> = result.take(0).unwrap();

            let Some(anchor_key) = follows.last().map(|follow| follow.id.key()) else {
                // sleep(DISCOVERY_CAUGHT_UP_BACKOFF).await;
                // continue;
                // TODO: Sleep again
                return Poll::Pending;
            };
            self.anchor = format!("{}", anchor_key);

            for follow in &follows {
                for record_id in [&follow.from, &follow.to] {
                    let did = unsafe_user_key_to_did(&format!("{}", record_id.key()));
                    if self.processed_dids.contains(&did) {
                        continue;
                    }
                    self.processed_dids.insert(did.clone());
                    self.buffer.push_back(did);
                    // tx.send(did)
                    //     .await
                    //     .context("Failed to send message to handler thread")?;
                }
            }

            if let Some(next) = self.buffer.pop_front() {
                return Poll::Ready(Some(next));
            }
            return Poll::Pending;

            // Warn if it looks like the queue size or the backoff were choosen incorrectly
            // let new_follows = self.processed_dids.len() - processed_dids_before;
            // if new_follows != 0 && follows.len() == fetch_amount && tx.len() < warning_threshold {
            //     warn!(target: "indexer", "Queue is not getting filled up fast enough. Consider increasing the queue size or decreasing the backoff.");
            // }
        }
    }
}
