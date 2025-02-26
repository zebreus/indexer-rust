apt update -y
apt install -y curl bash git btop htop nano clang llvm openssl libssl-dev pkg-config

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs >rustup.sh
sh rustup.sh -y
rm rustup.sh
. "$HOME/.cargo/env"
rustup toolchain install nightly
rustup default nightly

if ! git status; then
    git clone https://github.com/zebreus/indexer-rust
    cd indexer-rust
fi

export OTEL_EXPORTER_OTLP_ENDPOINT="http://monitoring.indexer.skyfeedlol.lol:39291"
echo 'export OTEL_EXPORTER_OTLP_ENDPOINT="http://monitoring.indexer.skyfeedlol.lol:39291"' >~/.bashrc

cargo build
echo 'Done! Run `./target/debug/indexer-rust --db "rocksdb:///root/rocks/db" --mode full` to start the indexer.'
