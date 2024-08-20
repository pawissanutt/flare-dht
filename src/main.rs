extern crate core;

use std::error::Error;
use std::net::SocketAddr;
use clap::Parser;
use cluster::{FlareNode, MapDescriptor};
use flare_dht::FlareOptions;
use network::raft_api::FlareRaftService;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use tracing_subscriber::util::SubscriberInitExt;
use types::flare::flare_raft_server::FlareRaftServer;
use crate::types::flare::flare_kv_server::FlareKvServer;
use crate::network::kv_api::FlareKvService;

mod dht;
mod discovery;
mod network;
mod types;
mod store;
mod raft;
mod shard;
mod util;
mod cluster;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let options = FlareOptions::parse();
    init_log();
    info!("use option {options:?}");
    if options.leader {

    }
    let node = FlareNode::new_leader(
        options.clone(), 
        vec![MapDescriptor{name:"test".into(), shard_ids: vec![0]}]
    );
    let kv = FlareKvService::new(&node);
    let mgnt = FlareRaftService::new(&node);

    let socket: SocketAddr = options.addr.parse()?;
    info!("start on {}", socket);
    Server::builder()
        .add_service(FlareKvServer::new(kv))
        .add_service(FlareRaftServer::new(mgnt))
        .serve(socket)
        .await?;

    Ok(())
}



fn init_log(){
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_thread_ids(true) // Enable thread ID logging
        .finish();
    subscriber.try_init()
        .expect("setting default subscriber failed");

}

