use crate::shard_raft::network::Network;
use crate::shard_raft::state_machine::KVAppStateMachine;
use crate::shard_raft::store::StateMachineStore;
use crate::shard_raft::{KvTypeConfig, Raft};
use crate::raft::log::MemLogStore;
use crate::raft::NodeId;
use openraft::Config;
use std::error::Error;
use std::sync::Arc;

use super::{KvShard, ShardId};

#[allow(dead_code)]
#[derive(Clone)]
pub struct RaftShard {
    pub node_id: NodeId,
    pub addr: String,
    pub raft: Raft,
    pub log_store: MemLogStore<KvTypeConfig>,
    pub state_machine_store: Arc<StateMachineStore<KVAppStateMachine>>,
    pub config: Arc<Config>,
}

impl RaftShard {
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

        RaftShard {
            node_id,
            addr: addr.into(),
            raft,
            log_store,
            state_machine_store,
            config,
        }
    }
}

impl KvShard for RaftShard  {
    async fn get(&self, key: &String) -> Option<Vec<u8>> {
        let state = self.state_machine_store.state_machine.read().await;
        return state.app_data.0.get(key).map(|v| v.clone());
    }

    async fn set(&self, key: String, value: Vec<u8>) -> Result<(), Box<dyn Error>> {
        let raft_request = crate::shard_raft::state_machine::FlareKvRequest::Set {
            key,
            value,
        };
        self
            .raft
            .client_write(raft_request)
            .await?;
        Ok(())
    }

    async fn delete(&self, key: &String) ->  Result<(), Box<dyn Error>> {
        let raft_request = crate::shard_raft::state_machine::FlareKvRequest::Delete {
            key: key.clone(),
        };
        self
            .raft
            .client_write(raft_request)
            .await?;
        Ok(())
    }
}