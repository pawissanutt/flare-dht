mod network;
mod network_tarpc;
pub mod state_machine;
mod store;

use openraft::Config;
use state_machine::FlareMetadataSM;
use std::str::FromStr;
use std::{io::Cursor, sync::Arc};
use store::StateMachineStore;
use tonic::transport::{Channel, Uri};
use tracing::info;

use crate::{
    proto::flare_control_client::FlareControlClient,
    raft::{log::MemLogStore, NodeId},
};

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub MetaTypeConfig:
        D = state_machine::FlareControlRequest,
        R = state_machine::FlareControlResponse,
);

pub type FlareMetaRaft = openraft::Raft<MetaTypeConfig>;

#[derive(Clone)]
pub struct FlareMetadataManager {
    pub raft: FlareMetaRaft,
    pub state_machine: Arc<StateMachineStore<FlareMetadataSM>>,
    pub node_id: NodeId,
}

impl FlareMetadataManager {
    pub async fn new(node_id: u64) -> Self {
        let config = Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        };
        info!("use config {:?}", config);
        let config = Arc::new(config.validate().unwrap());
        let log_store = MemLogStore::default();
        let sm: StateMachineStore<FlareMetadataSM> = store::StateMachineStore::default();
        let sm_arc = Arc::new(sm);
        let network = network::Network::new();
        // let network = network_tarpc::TarpcNetwork::default();

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
            node_id,
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

    #[inline]
    pub async fn is_current_voter(&self) -> bool {
        self.is_voter(self.node_id).await
    }

    #[inline]
    pub async fn is_voter(&self, node_id: u64) -> bool {
        let sm = self.state_machine.state_machine.read().await;
        sm.last_membership.voter_ids().any(|id| id == node_id)
    }

    #[inline]
    pub async fn is_leader(&self) -> bool {
        let leader_id = self.raft.current_leader().await;
        match leader_id {
            Some(id) => id == self.node_id,
            None => false,
        }
    }

    pub async fn create_control_client(&self) -> Option<FlareControlClient<Channel>> {
        let sm = self.state_machine.state_machine.read().await;
        self.raft.current_leader().await.map(|node_id| {
            let node = sm.last_membership.membership().get_node(&node_id).unwrap();
            let peer_addr: Uri = Uri::from_str(&node.addr).unwrap();
            let channel = Channel::builder(peer_addr).connect_lazy();
            FlareControlClient::new(channel)
        })
    }
}
