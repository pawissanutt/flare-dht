mod network;
pub mod state_machine;
mod store;
#[cfg(test)]
mod test;

use flare_pb::{
    ClusterMetadata, ClusterMetadataRequest, CreateCollectionRequest,
    CreateCollectionResponse, JoinRequest, JoinResponse, LeaveRequest,
};
use openraft::{BasicNode, ChangeMembers, Config};
use state_machine::{
    CollectionMetadata, FlareControlRequest, FlareControlResponse,
    FlareMetadataSM,
};
use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::time::Duration;
use std::u32;
use std::{io::Cursor, sync::Arc};
use store::StateMachineStore;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Uri};
use tracing::info;

use flare_pb::flare_control_client::FlareControlClient;

use crate::error::{FlareError, FlareInternalError};
use crate::pool::{AddrResolver, ClientPool};
use crate::raft::log::MemLogStore;
use crate::raft::NodeId;
use crate::shard::ShardMetadata;

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub MetaTypeConfig:
        D = state_machine::FlareControlRequest,
        R = state_machine::FlareControlResponse,
);

pub type FlareMetaRaft = openraft::Raft<MetaTypeConfig>;

mod typ {

    use crate::raft::NodeId;
    pub type RaftError<E = openraft::error::Infallible> =
        openraft::error::RaftError<NodeId, E>;
    pub type RPCError<E = openraft::error::Infallible> =
        openraft::error::RPCError<NodeId, openraft::BasicNode, RaftError<E>>;
}

#[async_trait::async_trait]
pub trait MetadataManager: Send + Sync {
    async fn initialize(&self) -> Result<(), FlareError>;
    async fn get_shard_ids(&self, col_name: &str) -> Option<Vec<u64>>;
    async fn get_shard_id(&self, col_name: &str, key: &str) -> Option<u64>;
    async fn leave(&self);
    async fn other_leave(&self, node_id: NodeId) -> Result<(), FlareError>;
    async fn other_join(
        &self,
        join_request: JoinRequest,
    ) -> Result<JoinResponse, FlareError>;
    async fn get_metadata(&self) -> Result<ClusterMetadata, FlareError>;

    async fn local_shards(&self) -> Vec<ShardMetadata>;
    async fn create_collection(
        &self,
        mut request: CreateCollectionRequest,
    ) -> Result<CreateCollectionResponse, FlareError>;

    fn create_watch(&self) -> tokio::sync::watch::Receiver<u64>;
}

struct RaftAddrResolver {
    state_machine: Arc<StateMachineStore<FlareMetadataSM>>,
}

#[async_trait::async_trait]
impl AddrResolver for RaftAddrResolver {
    async fn resolve(&self, node_id: NodeId) -> Option<String> {
        let sm = self.state_machine.state_machine.read().await;
        if let Some(node) = sm.last_membership.membership().get_node(&node_id) {
            Some(node.addr.clone())
        } else {
            None
        }
    }
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct FlareMetadataManager {
    pub client_pool: Arc<ClientPool>,
    pub node_id: NodeId,
    pub(crate) raft: FlareMetaRaft,
    pub(crate) state_machine: Arc<StateMachineStore<FlareMetadataSM>>,
    config: Arc<Config>,
    log_store: MemLogStore<MetaTypeConfig>,
    node_addr: String,
}

fn resolve_shard_id(meta: &CollectionMetadata, key: &str) -> Option<u64> {
    let hashed = mur3::murmurhash3_x86_32(key.as_bytes(), meta.seed) as u32;
    let shard_count = meta.shard_ids.len();
    let size = u32::div_ceil(u32::MAX, shard_count as u32);
    let shard_index = hashed / size;
    Some(meta.shard_ids[shard_index as usize])
}

impl FlareMetadataManager {
    pub async fn new(node_id: u64, node_addr: String) -> Self {
        let config = Config {
            ..Default::default()
        };
        info!("use raft {:?}", config);
        let config = Arc::new(config.validate().unwrap());
        let log_store = MemLogStore::default();
        let sm: StateMachineStore<FlareMetadataSM> =
            store::StateMachineStore::default();
        let sm_arc = Arc::new(sm);
        let network = network::Network::new();
        let resolver = RaftAddrResolver {
            state_machine: sm_arc.clone(),
        };
        let client_pool = Arc::new(ClientPool::new(Arc::new(resolver)));
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
            config,
            client_pool,
            log_store,
            node_addr,
        }
    }

    #[inline]
    async fn is_current_voter(&self) -> bool {
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

    #[inline]
    pub async fn get_leader_id(&self) -> Result<u64, FlareInternalError> {
        self.raft
            .current_leader()
            .await
            .ok_or(FlareInternalError::NoLeader)
    }

    pub async fn create_control_client(
        &self,
    ) -> Option<FlareControlClient<Channel>> {
        let sm = self.state_machine.state_machine.read().await;
        self.raft.current_leader().await.map(|node_id| {
            let node =
                sm.last_membership.membership().get_node(&node_id).unwrap();
            let peer_addr: Uri = Uri::from_str(&node.addr).unwrap();
            let channel = Channel::builder(peer_addr).connect_lazy();
            FlareControlClient::new(channel)
        })
    }

    pub async fn get_node_addr(&self, node_id: NodeId) -> Option<String> {
        let sm = self.state_machine.state_machine.read().await;
        if let Some(node) = sm.last_membership.membership().get_node(&node_id) {
            Some(node.addr.clone())
        } else {
            None
        }
    }
}

#[async_trait::async_trait]
impl MetadataManager for FlareMetadataManager {
    async fn initialize(&self) -> Result<(), FlareError> {
        let mut map = BTreeMap::new();
        map.insert(
            self.node_id,
            openraft::BasicNode {
                addr: self.node_addr.clone(),
            },
        );
        self.raft
            .initialize(map)
            .await
            .map_err(|e| FlareInternalError::RaftError(Box::new(e)))?;
        Ok(())
    }

    async fn get_shard_ids(&self, col_name: &str) -> Option<Vec<u64>> {
        let state_machine = self.state_machine.clone();
        let col_meta_state = state_machine.state_machine.read().await;
        let col = col_meta_state
            .app_data
            .collections
            .get(col_name)
            .map(|col| col.shard_ids.clone());
        col
    }

    async fn get_shard_id(&self, col_name: &str, key: &str) -> Option<u64> {
        let col_meta_state = self.state_machine.state_machine.read().await;
        let col = col_meta_state.app_data.collections.get(col_name);
        if let Some(meta) = col {
            resolve_shard_id(meta, key)
        } else {
            None
        }
    }

    async fn leave(&self) {
        let mut nodes = std::collections::BTreeSet::new();
        nodes.insert(self.node_id);
        if self.is_leader().await {
            let sm = self.state_machine.state_machine.read().await;
            let node_count = sm.last_membership.nodes().count();
            drop(sm);
            if node_count == 1 {
                // self.shutdown().await;
                return;
            } else {
                let change_members: ChangeMembers<u64, openraft::BasicNode> =
                    ChangeMembers::RemoveVoters(nodes);
                self.raft
                    .change_membership(change_members, true)
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_secs(2)).await;
                match self.create_control_client().await {
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
            }
        } else {
            let is_voter = self.is_current_voter().await;
            let mut client = self.create_control_client().await.unwrap();
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
    }

    async fn other_leave(&self, node_id: NodeId) -> Result<(), FlareError> {
        let mut nodes = BTreeSet::new();
        nodes.insert(node_id);
        if self.is_voter(node_id).await {
            let change_members = ChangeMembers::RemoveVoters(nodes);
            self.raft
                .change_membership(change_members, true)
                .await
                .map_err(|e| FlareError::UnknownError(Box::new(e)))?;
        } else {
            let change_members = ChangeMembers::RemoveNodes(nodes);
            self.raft
                .change_membership(change_members, true)
                .await
                .map_err(|e| FlareError::UnknownError(Box::new(e)))?;
        }
        Ok(())
    }

    async fn other_join(
        &self,
        join_request: JoinRequest,
    ) -> Result<JoinResponse, FlareError> {
        if !self.is_leader().await {
            let leader_id = self.get_leader_id().await?;
            let mut cc = self.client_pool.control_pool.get(leader_id).await?;
            return cc
                .join(join_request)
                .await
                .map(|r| r.into_inner())
                .map_err(|s| FlareError::RpcError(s));
        }

        let mut map = BTreeMap::new();
        let node = BasicNode {
            addr: join_request.addr.clone(),
        };
        let node_id = join_request.node_id;
        map.insert(node_id, node);
        let is_initialized = self.raft.is_initialized().await.unwrap();
        if is_initialized {
            let change_members = ChangeMembers::AddVoters(map);
            self.raft
                .change_membership(change_members, false)
                .await
                .map_err(|e| FlareInternalError::RaftError(Box::new(e)))?;
        } else {
            map.insert(
                self.node_id,
                BasicNode {
                    addr: self.node_addr.clone(),
                },
            );
            self.raft
                .initialize(map)
                .await
                .map_err(|e| FlareInternalError::RaftError(Box::new(e)))?;
        }
        Ok(JoinResponse::default())
    }

    async fn get_metadata(&self) -> Result<ClusterMetadata, FlareError> {
        if !self.is_leader().await {
            let leader_id = self.get_leader_id().await?;
            let mut cc = self
                .client_pool
                .control_pool
                .get(leader_id)
                .await
                .map_err(FlareError::from)?;
            return cc
                .get_metadata(ClusterMetadataRequest::default())
                .await
                .map(|r| r.into_inner())
                .map_err(|e| FlareError::RpcError(e));
        }

        self.raft
            .ensure_linearizable()
            .await
            .map_err(|e| FlareInternalError::RaftError(Box::new(e)))?;
        let state_machine = self.state_machine.state_machine.read().await;
        let metadata_sm = &state_machine.app_data;
        let collection_sm = &metadata_sm.collections;
        let mut collections = HashMap::with_capacity(collection_sm.len());
        for (name, col) in collection_sm.iter() {
            collections.insert(
                name.clone(),
                flare_pb::CollectionMetadata {
                    name: col.name.clone(),
                    shard_ids: col.shard_ids.clone(),
                    replication: col.replication as u32,
                },
            );
        }
        let mut shards = HashMap::with_capacity(metadata_sm.shards.len());
        let ssm = &metadata_sm.shards;
        for (id, shard) in ssm.iter() {
            shards.insert(
                *id,
                flare_pb::ShardMetadata {
                    id: shard.id,
                    collection: shard.collection.clone(),
                    primary: shard.primary,
                    replica: shard.replica.clone(),
                },
            );
        }

        let cm = flare_pb::ClusterMetadata {
            collections,
            shards,
            last_shard_id: metadata_sm.last_shard_id,
        };
        Ok(cm)
    }

    async fn local_shards(&self) -> Vec<ShardMetadata> {
        let sm = self.state_machine.state_machine.read().await;
        let local_shards = sm
            .app_data
            .shards
            .values()
            .filter(|shard| shard.primary.unwrap_or(0) == self.node_id)
            .cloned()
            .collect();
        local_shards
    }

    async fn create_collection(
        &self,
        mut request: CreateCollectionRequest,
    ) -> Result<CreateCollectionResponse, FlareError> {
        if request.shard_count == 0 {
            return Err(FlareError::InvalidArgument(
                "shard count must be positive".into(),
            ));
        }
        if !self.is_leader().await {
            let leader_id = self.get_leader_id().await?;
            let mut client = self.client_pool.kv_pool.get(leader_id).await?;
            let resp = client.create_collection(request).await?;
            return Ok(resp.into_inner());
        }
        if request.shard_assignments.len() != request.shard_count as usize {
            let node_id = self.node_id;
            request.shard_assignments =
                vec![node_id].repeat(request.shard_count as usize);
        }
        let req = FlareControlRequest::CreateCollection(request);
        let resp = self
            .raft
            .client_write(req)
            .await
            .map_err(|e| FlareError::UnknownError(Box::new(e)))?;
        if let FlareControlResponse::CollectionCreated { meta } =
            resp.response()
        {
            Ok(CreateCollectionResponse {
                name: meta.name.clone(),
            })
        } else {
            Err(FlareError::InvalidArgument(
                "collection already exist".into(),
            ))
        }
    }

    fn create_watch(&self) -> tokio::sync::watch::Receiver<u64> {
        let (tx, rx) = tokio::sync::watch::channel(0);
        let mut stream =
            tokio_stream::wrappers::WatchStream::new(self.raft.data_metrics());
        tokio::spawn(async move {
            loop {
                if let Some(d) = stream.next().await {
                    if let Some(log_id) = d.last_applied {
                        if let Err(_) = tx.send(log_id.index) {
                            break;
                        }
                    }
                }
            }
        });
        rx
    }
}

#[test]
pub fn test_resolve_shard2() -> Result<(), Box<dyn std::error::Error>> {
    let shard_count = 256;
    let meta = CollectionMetadata {
        name: "test".into(),
        shard_ids: (0..shard_count).collect(),
        replication: 1,
        seed: rand::random(),
    };
    for i in 0..1000000 {
        let option = resolve_shard_id(&meta, &format!("test-{}", i));
        assert_ne!(option, None);
        assert!(option.unwrap() < shard_count)
    }
    Ok(())
}
