use std::{fmt::Error, sync::Arc};
use std::collections::BTreeMap;
use dashmap::DashMap;

use crate::{shard::FlareShard, types::{NodeId, ShardId}, FlareOptions};

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
    pub shards: Vec<ShardId>,
}

#[derive(Clone, Default, Debug)]
pub struct MapDescriptor {
    pub name: String,
    pub shard_ids: Vec<ShardId>,
}

impl FlareNode {
    #[tracing::instrument]
    pub fn new_leader(options: FlareOptions, init: Vec<MapDescriptor>) -> Self {
        let map = DashMap::new();
        let shards = DashMap::new();
        let mut cluster_info = FlareClusterInfo::default();
        for i in init {
            map.insert(i.name.clone(), i);
        }
        cluster_info.node_info.insert(0, NodeInfo {
            id: 0,
            shards: vec![],
        });

        FlareNode {
            map_descriptors: Arc::new(map),
            shards: Arc::new(shards),
            addr: options.addr,
            node_id: 0,
            cluster_info,
        }
    }

    pub async fn init_shards(self) -> Result<(), Error> {
        for descriptor in self.map_descriptors.clone().iter() {
            for shard_id in descriptor.shard_ids.iter() {
                let shard = FlareShard::new(self.node_id, &self.addr, *shard_id).await;
                self.shards.insert(*shard_id, shard);
            }
        }
        Ok(())
    }
}