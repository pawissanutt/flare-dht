use std::{sync::Arc};
use std::collections::BTreeMap;
use std::error::Error;
use std::str::FromStr;
use dashmap::DashMap;
use openraft::BasicNode;
use tonic::transport::{Channel, Uri};
use tracing::info;
use crate::{shard::FlareShard, types::{NodeId, ShardId}, FlareOptions};
use crate::proto::flare_control_client::FlareControlClient;
use crate::proto::JoinRequest;

#[derive(Clone)]
pub struct FlareNode {
    pub map_descriptors: Arc<DashMap<String, MapDescriptor>>,
    pub shards: Arc<DashMap<ShardId, FlareShard>>,
    pub addr: String,
    pub node_id: NodeId,
    pub cluster_info: FlareClusterInfo,
}

#[derive(Clone, Default, Debug)]
pub struct FlareClusterInfo {
    node_info: BTreeMap<NodeId, NodeInfo>,
}

#[derive(Clone, Default, Debug)]
pub struct NodeInfo {
    pub id: NodeId,
    pub addr: String,
    pub shards: Vec<ShardId>,
}

#[derive(Clone, Default, Debug)]
pub struct MapDescriptor {
    pub name: String,
    pub shard_ids: Vec<ShardId>,
}

fn default_map() -> MapDescriptor {
    MapDescriptor{name:"default".into(), shard_ids: vec![0]}
}

impl FlareNode {
    #[tracing::instrument]
    pub fn new(options: FlareOptions, init: Vec<MapDescriptor>) -> Self {
        let map = DashMap::new();
        let shards = DashMap::new();
        let mut cluster_info = FlareClusterInfo::default();
        let node_id = options.get_node_id();
        info!("use node_id: {node_id}");
        let default_map = default_map();
        map.insert(default_map.name.clone(), default_map);
        for i in init {
            map.insert(i.name.clone(), i);
        }
        let addr = options.get_addr();
        cluster_info.node_info.insert(0, NodeInfo {
            id: node_id,
            shards: vec![],
            addr: addr.clone()
        });

        FlareNode {
            map_descriptors: Arc::new(map),
            shards: Arc::new(shards),
            addr,
            node_id,
            cluster_info,
        }
    }

    pub async fn init_shards(&self, init_raft: bool) -> Result<(), Box<dyn Error>> {
        for descriptor in self.map_descriptors.clone().iter() {
            for shard_id in descriptor.shard_ids.iter() {
                let shard = FlareShard::new(self.node_id, &self.addr, *shard_id).await;
                let mut nodes = BTreeMap::new();
                nodes.insert(self.node_id, BasicNode { addr: self.addr.clone() });
                if init_raft {
                    shard.raft.initialize(nodes).await?;
                }
                self.shards.insert(*shard_id, shard);
            }
        }
        Ok(())
    }

    pub async fn join(&self, peer_addr: &str) -> Result<(), Box<dyn Error>> {
        let addr = Uri::from_str(peer_addr)?;
        let channel = Channel::builder(addr)
            .connect_lazy();
        let mut client = FlareControlClient::new(channel);
        let resp = client.join(JoinRequest {
            node_id: self.node_id,
            addr: self.addr.clone(),
        }).await?;
        resp.into_inner();
        Ok(())
    }
}