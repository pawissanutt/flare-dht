use crate::kv::network::Network;
use crate::kv::state_machine::KVAppStateMachine;
use crate::kv::store::StateMachineStore;
use crate::kv::{KvTypeConfig, Raft};
use crate::raft::log::MemLogStore;
use crate::raft::NodeId;
use openraft::Config;
use std::sync::Arc;

pub type ShardId = u64;

#[allow(dead_code)]
#[derive(Clone)]
pub struct FlareShard {
    pub id: NodeId,
    pub addr: String,
    pub raft: Raft,
    pub log_store: MemLogStore<KvTypeConfig>,
    pub state_machine_store: Arc<StateMachineStore<KVAppStateMachine>>,
    pub config: Arc<Config>,
}

impl FlareShard {
    pub async fn new(node_id: NodeId, addr: &str, shard_id: ShardId) -> Self {
        let config = Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        };

        let config = Arc::new(config.validate().unwrap());
        let log_store = MemLogStore::default();
        let state_machine_store = Arc::new(StateMachineStore::default());
        let network = Network::new(shard_id);

        let raft = Raft::new(
            node_id,
            config.clone(),
            network,
            log_store.clone(),
            state_machine_store.clone(),
        )
        .await
        .unwrap();

        FlareShard {
            id: node_id,
            addr: addr.into(),
            raft,
            log_store,
            state_machine_store,
            config,
        }
    }
}
