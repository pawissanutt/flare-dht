mod network;
mod network_tarpc;
pub mod state_machine;
mod store;

use openraft::Config;
use state_machine::FlareMetadataSM;
use std::{io::Cursor, sync::Arc};
use store::StateMachineStore;

use crate::raft::log::MemLogStore;

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub MetaTypeConfig:
        D = state_machine::FlareControlRequest,
        R = state_machine::FlareControlResponse,
);

pub type FlareMetaRaft = openraft::Raft<MetaTypeConfig>;

pub struct FlareMetadataManager {
    pub raft: FlareMetaRaft,
    pub state_machine: Arc<StateMachineStore<FlareMetadataSM>>,
}

impl FlareMetadataManager {
    pub async fn new(node_id: u64) -> Self {
        let config = Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        };
        let config = Arc::new(config.validate().unwrap());
        let log_store = MemLogStore::default();
        let sm: StateMachineStore<FlareMetadataSM> = store::StateMachineStore::default();
        let sm_arc = Arc::new(sm);
        // let network = network::Network::new();
        let network = network_tarpc::TarpcNetwork::default();

        let raft = FlareMetaRaft::new(
            node_id,
            config.clone(),
            network,
            log_store.clone(),
            sm_arc.clone(),
        )
        .await
        .unwrap();

        FlareMetadataManager {
            raft,
            state_machine: sm_arc,
        }
    }

    pub async fn get_shard_ids(&self, col_name: &str) -> Option<Vec<u64>> {
        let state_machine = self.state_machine.clone();
        let col_meta_state = state_machine.state_machine.read().await;
        let col = col_meta_state
            .app_data
            .collections
            .get(col_name)
            .map(|col| col.shard_ids.clone());
        col
    }
}
