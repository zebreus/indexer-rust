use crate::{config::ARGS, database::utils::unsafe_user_key_to_did};
use futures::Stream;
use serde::Deserialize;
use std::{
    collections::{HashSet, VecDeque},
    future::{Future, IntoFuture},
    task::Poll,
};
use surrealdb::{engine::any::Any, Surreal};
use tracing::{error, trace};

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

#[allow(dead_code)]
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
            trace!("RepoStream buffer empty, fetching more data");

            // Get a running query or create a new db query
            let db_future = match &mut self.db_future {
                Some(db_future) => db_future,
                _ => {
                    let db_future = self
                        .db
                        // TODO: Fix the possible SQL injection
                        .query("SELECT of FROM latest_backfill WHERE at IS NONE LIMIT $limit;")
                        .bind(("limit", ARGS.repo_stream_buffer_size))
                        .into_owned()
                        .into_future();
                    self.db_future = Some(db_future);
                    self.db_future.as_mut().unwrap()
                }
            };

            let Poll::Ready(result) = Future::poll(db_future.as_mut(), cx) else {
                return Poll::Pending;
            };
            self.db_future = None;

            let mut result = match result {
                Ok(result) => result,
                Err(err) => {
                    error!("RepoStream error: {:?}", err);
                    continue;
                }
            };

            let follows: Vec<LatestBackfill> = match result.take(0) {
                Ok(follows) => follows,
                Err(err) => {
                    error!("RepoStream database error: {:?}", err);
                    continue;
                }
            };

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
            trace!(
                "RepoStream processed {} records in {}ms",
                follows.len(),
                duration.as_millis()
            );

            // Loop to see if we can return a value now
        }
    }
}
