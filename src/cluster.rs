use crate::metadata::FlareMetadataManager;
use crate::proto::flare_control_client::FlareControlClient;
use crate::proto::{JoinRequest, LeaveRequest};
use crate::shard::ShardId;
use crate::{raft::NodeId, shard::FlareShard, FlareOptions};
use dashmap::DashMap;
use openraft::ChangeMembers;
use std::collections::BTreeMap;
use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
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
    // #[tracing::instrument]
    pub async fn new(options: FlareOptions) -> Self {
        let shards = DashMap::new();
        let node_id = options.get_node_id();
        info!("use node_id: {node_id}");
        let addr = options.get_addr();
        let meta_raft = FlareMetadataManager::new(node_id).await;
        FlareNode {
            shards: Arc::new(shards),
            metadata_manager: Arc::new(meta_raft),
            addr,
            node_id,
        }
    }

    pub async fn init_leader(&self) -> Result<(), Box<dyn Error>> {
        let mm = self.metadata_manager.clone();
        let mut map = BTreeMap::new();
        map.insert(
            self.node_id,
            openraft::BasicNode {
                addr: self.addr.clone(),
            },
        );
        mm.raft.initialize(map).await?;
        Ok(())
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

    pub async fn leave(&self) {
        let mm = self.metadata_manager.clone();
        let mut nodes = std::collections::BTreeSet::new();
        nodes.insert(self.node_id);
        if mm.is_leader().await {
            let change_members: ChangeMembers<u64, openraft::BasicNode> =
                ChangeMembers::RemoveVoters(nodes);
            mm.raft
                .change_membership(change_members, true)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(2)).await;
            match mm.create_control_client().await {
                Some(mut client) => {
                    client
                        .leave(LeaveRequest {
                            node_id: self.node_id,
                        })
                        .await
                        .unwrap();
                }
                None => {}
            };
        } else {
            let is_voter = mm.is_current_voter().await;
            let mut client = mm.create_control_client().await.unwrap();
            client
                .leave(LeaveRequest {
                    node_id: self.node_id,
                })
                .await
                .unwrap();
            if is_voter {
                client
                    .leave(LeaveRequest {
                        node_id: self.node_id,
                    })
                    .await
                    .unwrap();
            }
        }
        info!("flare leave group");
    }
}
