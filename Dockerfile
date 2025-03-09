FROM rustlang/rust:nightly-slim AS builder

WORKDIR /app

# Download dependencies
COPY Cargo.lock .
COPY Cargo.toml .
COPY .cargo ./.cargo
RUN cargo fetch --locked

# Build a dummy project to cache dependencies
RUN mkdir src && echo "fn main() {}" >src/main.rs
RUN cargo build --locked --offline --release
RUN rm -rf src

# Copy the source code
COPY ISRG_Root_X1.pem .
COPY src ./src

# Build the project
RUN touch src/main.rs && cargo build --locked --offline --release

FROM rustlang/rust:nightly-slim AS indexer

COPY --from=builder /app/target/release/indexer /bin/indexer
ENTRYPOINT ["/bin/indexer"]
