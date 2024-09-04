extern crate core;

use clap::Parser;
use flare_dht::FlareOptions;
use std::error::Error;
use tokio::signal;
use tracing::info;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

mod cluster;
// mod shard_raft;
mod metadata;
mod proto;
mod raft;
mod rpc_server;
mod shard;
mod util;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    init_log();
    let options = FlareOptions::parse();
    info!("use option {options:?}");
    let flare_node = flare_dht::start_server(options).await?;

    match signal::ctrl_c().await {
        Ok(()) => {}
        Err(err) => {
            eprintln!("Unable to listen for shutdown signal: {}", err);
            // we also shut down in case of error
        }
    }
    info!("starting a clean up for shutdown");
    flare_node.leave().await;
    Ok(())
}

fn init_log() {
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_thread_ids(true) // Enable thread ID logging
        .finish();
    subscriber
        .try_init()
        .expect("setting default subscriber failed");
}
