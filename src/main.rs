extern crate core;

use crate::rpc_server::control_api::FlareControlService;
use crate::rpc_server::kv_api::FlareKvService;
use crate::rpc_server::raft_api::FlareMetaRaftService;
use clap::Parser;
use cluster::FlareNode;
use flare_dht::FlareOptions;
use proto::flare_metadata_raft_server::FlareMetadataRaftServer;
use proto::flare_control_server::FlareControlServer;
use proto::flare_kv_server::FlareKvServer;
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::signal;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

mod cluster;
mod shard_raft;
mod metadata;
mod proto;
mod raft;
mod rpc_server;
mod shard;
mod util;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let options = FlareOptions::parse();
    init_log();
    info!("use option {options:?}");
    let flare_node = FlareNode::new(options.clone()).await;
    if options.leader {
        flare_node.init_leader().await?;
    }
    let shared_node = Arc::new(flare_node);
    // let flare_node = shared_node.clone();
    // tokio::spawn(async move {
    //     rpc_server::raft2::start_server(flare_node, options.raft_port).await
    //     .unwrap();
    // });
    let flare_node = shared_node.clone();
    let flare_kv = FlareKvService::new(shared_node.clone());
    let flare_meta_raft = FlareMetaRaftService::new(shared_node.clone());
    let flare_control = FlareControlService::new(shared_node.clone());

    // let socket: SocketAddr = options.addr.parse()?;
    let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), options.port);
    info!("start on {}", socket);
    let reflection_server = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    drop(flare_node);

    if let Some(addr) = options.peer_addr {
        let flare_node = shared_node.clone();
        tokio::spawn(async move {
            let node = flare_node.clone();
            node.join(&addr).await.unwrap()
        });
    };
    
    tokio::spawn(async move {
        Server::builder()
            .add_service(reflection_server)
            .add_service(FlareKvServer::new(flare_kv))
            .add_service( FlareMetadataRaftServer::new(flare_meta_raft))
            .add_service(FlareControlServer::new(flare_control))
            .serve(socket)
            .await
            .unwrap();
    });

    match signal::ctrl_c().await {
        Ok(()) => {}
        Err(err) => {
            eprintln!("Unable to listen for shutdown signal: {}", err);
            // we also shut down in case of error
        }
    }
    info!("starting a clean up for shutdown");
    let flare_node = shared_node.clone();
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
