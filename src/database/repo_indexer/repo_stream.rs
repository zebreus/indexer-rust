use crate::{config::ARGS, database::utils::unsafe_user_key_to_did};
use chrono::{DateTime, Utc};
use futures::{FutureExt, Stream};
use sqlx::PgPool;
use std::{
    collections::{HashSet, VecDeque},
    future::{Future, IntoFuture},
    pin::Pin,
    task::Poll,
};
use tracing::{error, trace};

pub struct RepoStream {
    buffer: VecDeque<String>,
    processed_dids: HashSet<String>,
    db: sqlx::PgPool,
    db_future: Option<Pin<Box<dyn Future<Output = Result<Vec<DbBackfill>, sqlx::Error>> + Send>>>,
}

impl RepoStream {
    pub fn new(db: PgPool) -> Self {
        Self {
            buffer: VecDeque::new(),
            processed_dids: HashSet::new(),
            db,
            db_future: None,
        }
    }
}

struct DbBackfill {
    id: String,
    at: Option<DateTime<Utc>>,
    of_did_id: String,
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
                    // Totally unsafe cast where we create a static ref to self.db using transmute
                    let static_db_ref =
                        unsafe { std::mem::transmute::<&PgPool, &'static PgPool>(&self.db) };
                    let db_future = sqlx::query_as!(
                        DbBackfill,
                        "SELECT * FROM latest_backfill WHERE at IS NULL LIMIT $1",
                        &(*&ARGS.repo_stream_buffer_size as i64)
                    )
                    .fetch_all(static_db_ref)
                    .into_future()
                    .boxed();

                    self.db_future = Some(db_future);
                    self.db_future.as_mut().unwrap()
                }
            };

            let Poll::Ready(result) = Future::poll(db_future.as_mut(), cx) else {
                return Poll::Pending;
            };
            self.db_future = None;

            let follows = match result {
                Ok(result) => result,
                Err(err) => {
                    error!("RepoStream error: {:?}", err);
                    continue;
                }
            };

            let starttime = std::time::Instant::now();
            for latest_backfill in &follows {
                let key = latest_backfill.of_did_id.clone();
                if self.processed_dids.contains(&key) {
                    continue;
                }
                self.processed_dids.insert(key.clone());
                // TODO: Investigate if we can just use the RecordId directly
                let did = unsafe_user_key_to_did(&format!("{}", key));
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
