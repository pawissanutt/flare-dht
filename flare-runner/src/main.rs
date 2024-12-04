use std::error::Error;

use clap::Parser;
use flare_dht::cli::FlareCli;
use flare_runner::handle_cli;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    layer::SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    init_log();
    let cli = FlareCli::parse();
    handle_cli(cli).await
}

fn init_log() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .with_env_var("FLARE_LOG")
                .from_env_lossy(),
        )
        .init();
}
