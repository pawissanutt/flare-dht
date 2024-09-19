extern crate core;

use clap::Parser;
use flare_dht::{FlareCli, FlareCommands};
use std::error::Error;
use tokio::signal;
use tracing::info;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    init_log();
    let cli = FlareCli::parse();

    if let FlareCommands::Server(args) = cli.command {
        let flare_node = flare_dht::start_server(args).await?;

        match signal::ctrl_c().await {
            Ok(()) => {}
            Err(err) => {
                eprintln!("Unable to listen for shutdown signal: {}", err);
                // we also shut down in case of error
            }
        }
        info!("starting a clean up for shutdown");
        flare_node.leave().await;
        info!("done clean up");
    }

    Ok(())
}

fn init_log() {
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        // .with_thread_ids(true) // Enable thread ID logging
        .finish();
    subscriber
        .try_init()
        .expect("setting default subscriber failed");
}
