# SkyFeed Indexer

ATProto/Bluesky Indexer, powered by [SurrealDB](https://github.com/surrealdb/surrealdb) and [Jetstream](https://github.com/bluesky-social/jetstream), written in [Rust](https://www.rust-lang.org/).

The indexer attaches a websocket to a Jetstream endpoint and converts all received events to SurrealDB queries. Temporary outtages are handled by the cursor system, which allows the indexer to resume indexing from the last known event.

The database can then be used to run powerful queries on the network data or build advanced custom feeds. All skyfeed.xyz feeds are powered by this service.

## Installation

1. Install the latest stable rust compiler from [rustup.rs](https://rustup.rs/).
2. Install either onto your system or into a docker container a [SurrealDB](https://surrealdb.com/docs/surrealdb/installation/running).
3. Clone the repository and run `cargo build --release`.
4. Launch the indexer with `./target/release/skyfeed-indexer [--help]`.

You may need to increase the ulimit for the number of open files. You can do this by running `ulimit -n 1000000`.

## Debugging and profiling

For benchmarking during development us the `dev-lto` profile. It should provide a reasonable compromise between build-time and runtime performance. To run the indexer with the `dev-lto` profile run `cargo run --profile dev-lto`.

### tokio

You can use tokio-console to get more insights into what the tokio tasks are currently doing. To enable Just run `tokio-console` while the indexer is running.

### opentelemetry

The application uses opentelemetry for metrics, traces, and logs. It exports signal via the OTLP grpc protocol. You can configure the exporter with the usual opentelemetry environment variables. The spin up a docker container with a collector and grafana use:

```
docker run -p 3000:3000 -p 4317:4317 --rm -ti grafana/otel-lgtm
```

and then visit `localhost:3000`. To disable opentelemetry use the `--no-otel-logs` and `--no-otel-metrics` flags.
