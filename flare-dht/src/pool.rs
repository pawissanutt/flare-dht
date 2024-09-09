use flare_pb::{flare_control_client::FlareControlClient, flare_kv_client::FlareKvClient};
use tonic::transport::{Channel, Uri};
use std::{str::FromStr, sync::Arc};
use crate::{error::FlareInternalError, metadata::FlareMetadataManager, raft::NodeId};

#[derive(Clone)]
pub struct ClientPool {
    pools: dashmap::DashMap<NodeId, Channel>,
    mm: Arc<FlareMetadataManager>,
}

type Result<T> = std::result::Result<T, FlareInternalError>;

impl ClientPool {
    pub fn new (mm: Arc<FlareMetadataManager>) -> Self {
        ClientPool{
            mm,
            pools: dashmap::DashMap::new()
        }
    }

    pub async  fn get(&self, node_id: NodeId) -> Result<Channel>{
        if let Some(c) = self.pools.get(&node_id) {
            return Ok(c.clone());
        } 
        if let Some(addr) = self.mm.get_node_addr(node_id).await {
            let ch = create_channel(&addr)?;
            self.pools.insert(node_id, ch.clone());
            Ok(ch)
        } else {
            Err(FlareInternalError::NoSuchNode(node_id))
        }
    }

    #[inline]
    pub async fn get_leader(&self) -> Result<Channel>{
        let node_id = self.mm.get_leader_id().await?;
        self.get(node_id).await
    }
    
    #[inline]
    pub async fn get_control_client(&self) -> Result<FlareControlClient<Channel>>{
        let channel = self.get_leader().await?;
        Ok(FlareControlClient::new(channel))
    }

    #[inline]
    pub async fn get_kv_client(&self, node_id: NodeId) -> Result<FlareKvClient<Channel>>{
        let channel = self.get(node_id).await?;
        Ok(FlareKvClient::new(channel))
    }
}

#[inline]
fn create_channel(addr: &str) -> Result<Channel>{
    let peer_uri: Uri = Uri::from_str(addr)?;
    let channel = Channel::builder(peer_uri).connect_lazy();
    Ok(channel)
}