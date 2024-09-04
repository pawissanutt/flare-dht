use std::{
    error::Error,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use clap::Parser;
use cluster::FlareNode;
use proto::{
    flare_control_server::FlareControlServer, flare_kv_server::FlareKvServer,
    flare_metadata_raft_server::FlareMetadataRaftServer,
};
use rpc_server::{
    control_api::FlareControlService, kv_api::FlareKvService, raft_api::FlareMetaRaftService,
};
use tonic::transport::Server;
use tracing::info;

pub mod cluster;
// mod shard_raft;
mod metadata;
pub mod proto;
mod raft;
pub mod rpc_server;
pub mod shard;
mod util;

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct FlareOptions {
    pub addr: Option<String>,
    #[arg(short, long, default_value = "8001")]
    pub port: u16,
    // #[arg(short, long, default_value = "18001")]
    // pub raft_port: u16,
    #[arg(short, long)]
    pub leader: bool,
    #[arg(long)]
    pub peer_addr: Option<String>,
    #[arg(short, long)]
    pub node_id: Option<u64>,
}

impl FlareOptions {
    pub fn get_node_id(&self) -> u64 {
        if let Some(id) = self.node_id {
            id
        } else {
            rand::random()
        }
    }

    pub fn get_addr(&self) -> String {
        if let Some(addr) = &self.addr {
            addr.clone()
        } else {
            format!("http://127.0.0.1:{}", self.port)
            // format!("127.0.0.1:{}", self.raft_port)
        }
    }
}

pub async fn start_server(options: FlareOptions) -> Result<Arc<FlareNode>, Box<dyn Error>> {
    info!("use option {options:?}");
    let flare_node = FlareNode::new(options.clone()).await;
    if options.leader {
        flare_node.init_leader().await?;
    }
    let shared_node = Arc::new(flare_node);
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
            .add_service(FlareMetadataRaftServer::new(flare_meta_raft))
            .add_service(FlareControlServer::new(flare_control))
            .serve(socket)
            .await
            .unwrap();
    });

    Ok(shared_node)
}
