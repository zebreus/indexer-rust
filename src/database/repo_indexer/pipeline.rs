use crate::config::ARGS;
use futures::FutureExt;
use opentelemetry::{
    global,
    metrics::{Counter, Histogram, UpDownCounter},
    KeyValue,
};
use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, LazyLock},
};
use tracing::{error, trace};

pub struct NoNextStage {}
pub trait NextStage {
    const NAME: &'static str;
    const DONE: bool;
}
impl<T: Stage> NextStage for T {
    const NAME: &'static str = T::NAME;
    const DONE: bool = false;
}
impl NextStage for NoNextStage {
    const NAME: &'static str = "Done";
    const DONE: bool = true;
}

pub trait Stage {
    type Output: NextStage + Sync + Send + 'static;
    const NAME: &'static str;
    const FIRST: bool = false;
    fn run(self) -> impl Future<Output = anyhow::Result<Self::Output>> + Send + Sync + 'static;
}

pub struct FirstStage<
    I: Sync + Send + 'static,
    O: Stage + Sync + Send + 'static,
    F: Fn(I) -> O + Sync + Send + 'static,
> {
    a: I,
    b: PhantomData<O>,
    f: Arc<F>,
}
impl<
        I: Sync + Send + 'static,
        O: Stage + Sync + Send + 'static,
        F: Fn(I) -> O + Sync + Send + 'static,
    > Stage for FirstStage<I, O, F>
{
    type Output = O;
    const NAME: &'static str = "First";
    const FIRST: bool = true;
    fn run(self) -> impl Future<Output = anyhow::Result<Self::Output>> + Send + Sync + 'static {
        async move { Ok((self.f)(self.a)) }
    }
}

pub fn create_stage<
    I: Sync + Send + 'static,
    O: Stage + Sync + Send + 'static,
    F: Fn(I) -> O + Sync + Send + 'static,
>(
    f: F,
) -> impl Fn(I) -> Pin<Box<dyn Future<Output = Option<O>> + Send + 'static>> {
    let next_stage_fn = next_stage::<FirstStage<I, O, F>>();
    let boxedfn = Arc::new(f);

    return move |x| {
        let first_stage = FirstStage::<I, O, F> {
            a: x,
            b: PhantomData,
            f: boxedfn.clone(),
        };
        return (next_stage_fn)(first_stage);
    };
}

pub fn next_stage<FROM: Stage>(
) -> impl Fn(FROM) -> Pin<Box<dyn Future<Output = Option<FROM::Output>> + Send + 'static>>
where
    FROM: Send + Sync + 'static + Stage,
    FROM::Output: Send + Sync + 'static,
{
    static TRACKER: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
        global::meter("indexer")
            .i64_up_down_counter("indexer.pipeline.location")
            .with_description("Track the number of tasks in the pipeline")
            .with_unit("tasks")
            .build()
    });
    static RUNTIME_METRIC: LazyLock<Histogram<u64>> = LazyLock::new(|| {
        global::meter("indexer")
            .u64_histogram("indexer.pipeline.duration")
            .with_unit("ms")
            .with_description("Pipeline job duration")
            .with_boundaries(vec![
                0.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 2500.0,
                5000.0, 7500.0, 10000.0, 25000.0, 50000.0, 75000.0, 100000.0, 250000.0, 500000.0,
                750000.0, 1000000.0, 2500000.0,
            ])
            .build()
    });
    static COMPLETED: LazyLock<Counter<u64>> = LazyLock::new(|| {
        global::meter("indexer")
            .u64_counter("indexer.pipeline.completed")
            .with_description("Pipelines finished")
            .with_unit("tasks")
            .build()
    });
    static FAILED: LazyLock<Counter<u64>> = LazyLock::new(|| {
        global::meter("indexer")
            .u64_counter("indexer.pipeline.failed")
            .with_description("Pipelines failed")
            .with_unit("tasks")
            .build()
    });
    return |x: FROM| {
        async move {
            tokio::task::spawn(async move {
                // Move from queued to active
                if !FROM::FIRST {
                    TRACKER.add(
                        -1,
                        &[
                            KeyValue::new("stage", FROM::NAME),
                            KeyValue::new("state", "queued"),
                        ],
                    );
                }
                TRACKER.add(
                    1,
                    &[
                        KeyValue::new("stage", FROM::NAME),
                        KeyValue::new("state", "active"),
                    ],
                );

                // Run the stage
                let before = std::time::Instant::now();
                let result = tokio::time::timeout(
                    tokio::time::Duration::from_secs(ARGS.pipeline_stage_timeout),
                    x.run(),
                )
                .await;
                let duration = before.elapsed();

                // Move away from active
                TRACKER.add(
                    -1,
                    &[
                        KeyValue::new("stage", FROM::NAME),
                        KeyValue::new("state", "active"),
                    ],
                );

                // Check if the stage timed out
                let Ok(result) = result else {
                    error!(
                        "Pipeline stage {} timed out in {:02}. Please adjust the timeout",
                        FROM::NAME,
                        duration.as_millis() as u64
                    );
                    FAILED.add(
                        1,
                        &[
                            KeyValue::new("stage", FROM::NAME),
                            KeyValue::new("reason", "timeout"),
                        ],
                    );
                    RUNTIME_METRIC.record(
                        duration.as_millis() as u64,
                        &[
                            KeyValue::new("stage", FROM::NAME),
                            KeyValue::new("result", "timeout"),
                        ],
                    );
                    return None;
                };

                // Check if the stage failed
                let result = match result {
                    Err(error) => {
                        error!(
                            "Pipeline stage {} failed in {:02} with error: {}",
                            FROM::NAME,
                            duration.as_millis() as f64 / 1000.0,
                            error
                        );
                        FAILED.add(
                            1,
                            &[
                                KeyValue::new("stage", FROM::NAME),
                                KeyValue::new("reason", "error"),
                            ],
                        );
                        RUNTIME_METRIC.record(
                            duration.as_millis() as u64,
                            &[
                                KeyValue::new("stage", FROM::NAME),
                                KeyValue::new("result", "error"),
                            ],
                        );
                        // error!(target: "indexer", "Failed to index repo: {}", error);
                        return None;
                    }
                    Ok(result) => result,
                };

                // If we are done, we track as a completed pipeline. Otherwise track as queued for the next stage.
                if !FROM::Output::DONE {
                    TRACKER.add(
                        1,
                        &[
                            KeyValue::new("stage", FROM::Output::NAME),
                            KeyValue::new("state", "queued"),
                        ],
                    );
                } else {
                    COMPLETED.add(1, &[]);
                }
                RUNTIME_METRIC.record(
                    duration.as_millis() as u64,
                    &[
                        KeyValue::new("stage", FROM::NAME),
                        KeyValue::new("result", "ok"),
                    ],
                );
                trace!(
                    "Pipeline stage {} finished in {:02}",
                    FROM::NAME,
                    duration.as_millis() as f64 / 1000.0
                );

                return Some(result);
            })
            .await
            .expect("Failed to spawn task in a pump stage. This is a hard error and means that something is wrong with your system. Maybe go buy a bigger machine or something?")
        }
        .boxed()
    };
}
