extern crate core;

use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::rc::Rc;
use std::sync::{Arc};
use clap::Parser;
use tokio::sync::Mutex;
use cluster::{FlareNode, MapDescriptor};
use flare_dht::FlareOptions;
use network::control_api::FlareControlService;
use network::raft_api::FlareRaftService;
use proto::flare_control_server::FlareControlServer;
use tonic::transport::Server;
use tonic_reflection::server::ServerReflectionServer;
use tracing::info;
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use tracing_subscriber::util::SubscriberInitExt;
use proto::flare_raft_server::FlareRaftServer;
use proto::flare_kv_server::FlareKvServer;
use crate::network::kv_api::FlareKvService;

pub mod proto {
    tonic::include_proto!("flare"); // The string specified here must match the proto package name
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("flare_descriptor");
}
mod discovery;
mod network;
mod types;
mod raft;
mod shard;
mod util;
mod cluster;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let options = FlareOptions::parse();
    init_log();
    info!("use option {options:?}");
    if options.leader {}
    let node = FlareNode::new(
        options.clone(),
        vec![],
    );
    let shared_node = Arc::new(Mutex::new(node));
    let node = shared_node.lock().await;
    node.init_shards(options.leader).await?;
    let kv = FlareKvService::new(&node);
    let mgnt = FlareRaftService::new(&node);
    let control = FlareControlService::new(shared_node.clone());

    // let socket: SocketAddr = options.addr.parse()?;
    let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), options.port);
    info!("start on {}", socket);
    let reflection_server = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build().unwrap();


    Server::builder()
        .add_service(reflection_server)
        .add_service(FlareKvServer::new(kv))
        .add_service(FlareRaftServer::new(mgnt))
        .add_service(FlareControlServer::new(control))
        .serve(socket)
        .await?;

    if let Some(addr) = options.peer_addr {
        node.join(&addr).await?;
    };
    Ok(())
}


fn init_log() {
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_thread_ids(true) // Enable thread ID logging
        .finish();
    subscriber.try_init()
        .expect("setting default subscriber failed");
}

