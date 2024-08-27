extern crate core;

use crate::rpc_server::control_api::FlareControlService;
use crate::rpc_server::kv_api::FlareKvService;
use crate::rpc_server::raft_api::FlareRaftService;
use clap::Parser;
use cluster::FlareNode;
use flare_dht::FlareOptions;
use proto::flare_control_server::FlareControlServer;
use proto::flare_kv_server::FlareKvServer;
use proto::flare_raft_server::FlareRaftServer;
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

pub mod proto {
    tonic::include_proto!("flare"); // The string specified here must match the proto package name
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("flare_descriptor");
}

mod cluster;
mod kv;
mod metadata;
mod raft;
mod rpc_server;
mod shard;
mod util;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let options = FlareOptions::parse();
    init_log();
    info!("use option {options:?}");
    if options.leader {}
    let flare_node = FlareNode::new(options.clone()).await;
    let shared_node = Arc::new(flare_node);
    let flare_node = shared_node.clone();
    tokio::spawn(async move {
        rpc_server::raft2::start_server(flare_node, options.raft_port).await
        .unwrap();   
    });
    let flare_node = shared_node.clone();
    let flare_kv = FlareKvService::new(&flare_node);
    let flare_raft_service = FlareRaftService::new(shared_node.clone());
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
        tokio::spawn(async move {
            let node = shared_node.clone();
            node.join(&addr).await.unwrap()
        });
    };

    Server::builder()
        .add_service(reflection_server)
        .add_service(FlareKvServer::new(flare_kv))
        .add_service(FlareRaftServer::new(flare_raft_service))
        .add_service(FlareControlServer::new(flare_control))
        .serve(socket)
        .await?;

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
