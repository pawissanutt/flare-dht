use std::sync::Arc;
use openraft::Config;
use crate::raft::Network;
use crate::store::StateMachineStore;
use crate::store::log::LogStore;
use crate::types::{NodeId, Raft, ShardId, TypeConfig};


#[allow(dead_code)]
#[derive(Clone)]
pub struct FlareShard{
    pub id: NodeId,
    pub addr: String,
    pub raft: Raft,
    pub log_store: LogStore<TypeConfig>,
    pub state_machine_store: Arc<StateMachineStore>,
    pub config: Arc<openraft::Config>,
}

impl FlareShard {
    pub async fn new(
        node_id: NodeId,
        addr: &str,
        shard_id: ShardId
    ) -> Self {
        let config = Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        };

        let config = Arc::new(config.validate().unwrap());
        let log_store = LogStore::default();
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

        FlareShard{
            id: node_id,
            addr: addr.into(),
            raft,
            log_store,
            state_machine_store,
            config,
        }
    }
}