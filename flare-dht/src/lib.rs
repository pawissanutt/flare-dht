pub use cluster::FlareNode;
use std::{
    error::Error,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use flare_pb::{
    flare_control_server::FlareControlServer, flare_kv_server::FlareKvServer,
    flare_metadata_raft_server::FlareMetadataRaftServer,
};
use rpc_server::{
    control_api::FlareControlService, kv_api::FlareKvService,
    raft_api::FlareMetaRaftService,
};
use tonic::transport::Server;
use tracing::info;

#[cfg(feature = "cluster")]
pub mod cluster;
mod metadata;
mod raft;
#[cfg(feature = "cluster")]
pub mod rpc_server;
pub mod shard;
mod util;
mod pool;
mod error;

#[derive(clap::Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct FlareCli {
    #[command(subcommand)]
    pub command: FlareCommands,
}

#[derive(clap::Subcommand, Clone, Debug)]
pub enum FlareCommands {
    /// Start as server
    Server(ServerArgs),
    Cli,
}

#[derive(clap::Args, Debug, Clone, Default)]
pub struct ServerArgs {
    /// advertisement address
    pub addr: Option<String>,
    /// gRPC port
    #[arg(short, long, default_value = "8001")]
    pub port: u16,
    /// if start as Raft leader
    #[arg(short, long)]
    pub leader: bool,
    #[arg(long, default_value = "false")]
    pub not_server: bool,
    /// Address to join the Raft cluster
    #[arg(long)]
    pub peer_addr: Option<String>,
    /// Node ID. Randomized, if none.
    #[arg(short, long)]
    pub node_id: Option<u64>,
}

impl ServerArgs {
    pub fn get_node_id(&self) -> u64 {
        if let Some(id) = self.node_id {
            return id;
        }
        rand::random()
    }

    pub fn get_addr(&self) -> String {
        if let Some(addr) = &self.addr {
            return addr.clone();
        }
        return format!("http://127.0.0.1:{}", self.port);
    }
}

pub async fn start_server(
    options: ServerArgs,
) -> Result<Arc<FlareNode>, Box<dyn Error>> {
    info!("use option {options:?}");
    let flare_node = FlareNode::new(options.clone()).await;
    if options.leader {
        flare_node.init_leader().await?;
    }
    let shared_node = Arc::new(flare_node);
    let flare_node = shared_node.clone();
    flare_node.start_watch_stream();
    let flare_node = shared_node.clone();
    let flare_kv = FlareKvService::new(shared_node.clone());
    let flare_meta_raft = FlareMetaRaftService::new(shared_node.clone());
    let flare_control = FlareControlService::new(shared_node.clone());

    // let socket: SocketAddr = options.addr.parse()?;
    if !options.not_server {
        let socket = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            options.port,
        );
        info!("start on {}", socket);
        let reflection_server_v1a = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(flare_pb::FILE_DESCRIPTOR_SET)
            .build_v1alpha()
            .unwrap();
        
        let reflection_server_v1 = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(flare_pb::FILE_DESCRIPTOR_SET)
            .build_v1()
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
                .add_service(reflection_server_v1a)
                .add_service(reflection_server_v1)
                .add_service(FlareKvServer::new(flare_kv))
                .add_service(FlareMetadataRaftServer::new(flare_meta_raft))
                .add_service(FlareControlServer::new(flare_control))
                .serve(socket)
                .await
                .unwrap();
        });
    }

    Ok(shared_node)
}
