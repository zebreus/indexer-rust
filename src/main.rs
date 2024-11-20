use std::sync::atomic::{AtomicUsize, Ordering};

use clap::{value_parser, Arg, ArgAction, Command};
use colog::format::CologStyle;
use colored::Colorize;
use log::{error, info, Level, LevelFilter};
use rayon::ThreadPoolBuilder;
use tokio::runtime::Builder;

use mimalloc::MiMalloc;

mod application;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub struct CustomPrefixToken;

impl CologStyle for CustomPrefixToken {
    fn prefix_token(&self, level: &Level) -> String {
        format!(
            "[{}] [{}]",
            chrono::Local::now().format("%d.%m.%Y %H:%M:%S"),
            match level {
                Level::Error => "E".red(),
                Level::Warn => "W".yellow(),
                Level::Info => "*".green(),
                Level::Debug => "D".blue(),
                Level::Trace => "T".purple(),
            }
        )
    }
}

fn main() {
    const DEFAULT_HOST: &str = "jetstream.de-4.skyfeed.network";
    const DEFAULT_CERT: &str = "/etc/ssl/certs/ISRG_Root_X1.pem";

    // parse command line arguments
    let cmd = Command::new("jetstream")
        .about("bluesky jetstream client")
        .version(env!("CARGO_PKG_VERSION"))
        .author("PancakeTAS")
        .arg(
            Arg::new("host")
                .long("host")
                .help("The jetstream host to connect to")
                .default_value(DEFAULT_HOST)
                .action(ArgAction::Set)
        )
        .arg(
            Arg::new("cert")
                .long("cert")
                .help("The certificate to use for the connection")
                .default_value(DEFAULT_CERT)
                .action(ArgAction::Set)
        )
        .arg(
            Arg::new("async-threads")
                .long("async-threads")
                .help("The number of threads to use for the websocket and other async operations")
                .value_parser(value_parser!(usize))
                .default_value("0") // 0 means use all available cores
                .action(ArgAction::Set)
        )
        .arg(
            Arg::new("parse-threads")
                .long("parse-threads")
                .help("The number of threads to use for parsing messages")
                .value_parser(value_parser!(usize))
                .default_value("0")
                .action(ArgAction::Set)
        )
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .help("Enable verbose output")
                .action(ArgAction::SetTrue)
        )
        .arg(
            Arg::new("cursor")
                .long("cursor")
                .help("Specify a unix microseconds timestamp to start playback from")
                .value_parser(value_parser!(u64))
                .default_value("0")
                .action(ArgAction::Set)
        );

    let matches = cmd.get_matches();
    let host: &String = matches.get_one("host")
        .expect("invalid value for --host");
    let cert: &String = matches.get_one("cert")
        .expect("invalid value for --cert");
    let async_threads: usize = *matches.get_one("async-threads")
        .expect("invalid value for --async-threads");
    let parse_threads: usize = *matches.get_one("parse-threads")
        .expect("invalid value for --parse-threads");
    let cursor: u64 = *matches.get_one("cursor")
        .expect("invalid value for --cursor");
    let verbose = matches.get_flag("verbose");

    // initialize logging
    let loglevel = if verbose { LevelFilter::Debug } else { LevelFilter::Info };
    colog::default_builder()
        .filter_level(LevelFilter::Off)
        .filter_module("jetstream", loglevel)
        .format(colog::formatter(CustomPrefixToken))
        .init();
    info!(target: "jetstream",
        "=============================================\n\
        jetstream client - v{}\n\
        =============================================\n\
        \n\
        host:          {}\n\
        certificate:   {}\n\
        async threads: {}\n\
        parse threads: {}\n\
        cursor:        {}\n\
        \n",
        env!("CARGO_PKG_VERSION"),
        host, cert, async_threads, parse_threads, cursor
    );

    // create global rayon thread pool
    ThreadPoolBuilder::new()
        .num_threads(parse_threads)
        .thread_name(|i| format!("parse-{}", i))
        .build_global()
        .unwrap();

    // build tokio runtime
    let mut builder = Builder::new_multi_thread();
    builder.enable_all();
    builder.thread_name_fn(|| {
        static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
        let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
        format!("async-{}", id)
    });
    if async_threads > 0 {
        builder.worker_threads(async_threads);
    }

    let rt = builder.build().unwrap();

    // launch async main
    let main = rt.block_on(
        application::launch_client(host, cert, cursor));
    if main.is_err() {
        error!(target: "jetstream", "{:?}", main.err().unwrap());
    }
}
