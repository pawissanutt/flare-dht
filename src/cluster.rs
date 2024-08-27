use crate::metadata::FlareMetadataManager;
use crate::proto::flare_control_client::FlareControlClient;
use crate::proto::JoinRequest;
use crate::shard::ShardId;
use crate::{raft::NodeId, shard::FlareShard, FlareOptions};
use dashmap::DashMap;
use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;
use tonic::transport::{Channel, Uri};
use tracing::info;

#[derive(Clone)]
pub struct FlareNode {
    pub metadata_manager: Arc<FlareMetadataManager>,
    pub shards: Arc<DashMap<ShardId, FlareShard>>,
    pub addr: String,
    pub node_id: NodeId,
}

impl FlareNode {
    #[tracing::instrument]
    pub async fn new(options: FlareOptions) -> Self {
        let shards = DashMap::new();
        let node_id = options.get_node_id();
        info!("use node_id: {node_id}");
        let addr = options.get_peer_addr();
        let meta_raft = FlareMetadataManager::new(node_id).await;

        FlareNode {
            shards: Arc::new(shards),
            metadata_manager: Arc::new(meta_raft),
            addr,
            node_id,
        }
    }

    pub async fn join(&self, peer_addr: &str) -> Result<(), Box<dyn Error>> {
        info!("advertise addr {}", self.addr);
        let peer_addr: Uri = Uri::from_str(peer_addr)?;
        let channel = Channel::builder(peer_addr).connect_lazy();
        let mut client = FlareControlClient::new(channel);
        let resp = client
            .join(JoinRequest {
                node_id: self.node_id,
                addr: self.addr.clone(),
            })
            .await?;
        resp.into_inner();
        Ok(())
    }
}