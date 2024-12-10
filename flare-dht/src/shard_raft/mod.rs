use network::Network;
use openraft::Config;
use state_machine::{FlareKvRequest, FlareKvResponse, KVAppStateMachine};
use store::StateMachineStore;
pub mod state_machine;
pub mod store;
use std::{error::Error, io::Cursor, sync::Arc};

use crate::{
    raft::{log::MemLogStore, NodeId},
    shard::{KvShard, ShardId},
};

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub KvTypeConfig:
        D = FlareKvRequest,
        R = FlareKvResponse,
);

pub type Raft = openraft::Raft<KvTypeConfig>;

#[allow(dead_code)]
pub mod typ {
    use crate::raft::NodeId;
    use crate::shard_raft::KvTypeConfig;
    use openraft::BasicNode;

    pub type RaftError<E = openraft::error::Infallible> =
        openraft::error::RaftError<NodeId, E>;
    pub type RPCError<E = openraft::error::Infallible> =
        openraft::error::RPCError<NodeId, BasicNode, RaftError<E>>;

    pub type ClientWriteError =
        openraft::error::ClientWriteError<NodeId, BasicNode>;
    pub type CheckIsLeaderError =
        openraft::error::CheckIsLeaderError<NodeId, BasicNode>;
    pub type ForwardToLeader =
        openraft::error::ForwardToLeader<NodeId, BasicNode>;
    pub type InitializeError =
        openraft::error::InitializeError<NodeId, BasicNode>;

    pub type ClientWriteResponse =
        openraft::raft::ClientWriteResponse<KvTypeConfig>;
}

#[allow(dead_code)]
#[derive(Clone)]
pub struct RaftShard {
    pub node_id: crate::raft::NodeId,
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

impl KvShard for RaftShard {
    async fn get(&self, key: &String) -> Option<Vec<u8>> {
        let state = self.state_machine_store.state_machine.read().await;
        return state.app_data.0.get(key).map(|v| v.clone());
    }

    async fn set(
        &self,
        key: String,
        value: Vec<u8>,
    ) -> Result<(), Box<dyn Error>> {
        let raft_request =
            crate::shard_raft::state_machine::FlareKvRequest::Set {
                key,
                value,
            };
        self.raft.client_write(raft_request).await?;
        Ok(())
    }

    async fn delete(&self, key: &String) -> Result<(), Box<dyn Error>> {
        let raft_request =
            crate::shard_raft::state_machine::FlareKvRequest::Delete {
                key: key.clone(),
            };
        self.raft.client_write(raft_request).await?;
        Ok(())
    }
}
