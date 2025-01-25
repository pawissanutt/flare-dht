mod state_machine;
#[cfg(test)]
mod test;

use flare_pb::flare_control_client::FlareControlClient;
use flare_pb::{ShardAssignment, ShardGroup};
use http::Uri;
use openraft::{BasicNode, ChangeMembers, Config};
use state_machine::FlareMetadataSM;
use std::collections::HashMap;
use std::str::FromStr;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tracing::info;
use zenoh::Session;

use crate::proto::{
    ClusterMetadata, ClusterMetadataRequest, CreateCollectionRequest,
    CreateCollectionResponse, JoinRequest, JoinResponse, LeaveRequest,
};
use crate::raft::generic::LocalStateMachineStore;
use crate::raft::log::MemLogStore;
use crate::raft::rpc::{Network, RaftZrpcService};
use crate::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use std::{io::Cursor, sync::Arc};

use crate::cli::ServerArgs;
use crate::error::{FlareError, FlareInternalError};
use crate::pool::{
    create_control_pool, create_data_pool, ControlPool, DataPool,
};

use super::{CollectionMetadataState, MetadataManager, ShardGroupState};
use crate::shard::ShardMetadata;

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub MetaTypeConfig:
        D = FlareControlRequest,
        R = FlareControlResponse,
);

pub type FlareMetaRaft = openraft::Raft<MetaTypeConfig>;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub enum FlareControlRequest {
    CreateCollection(CreateCollectionRequest),
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub enum FlareControlResponse {
    CollectionCreated { meta: CollectionMetadataState },
    Rejected(String),
    Empty,
}

#[allow(dead_code)]
pub struct FlareMetadataManager {
    // pub client_pool: Arc<ClientPool>,
    pub control_pool: Arc<ControlPool>,
    pub data_pool: Arc<DataPool>,
    pub node_id: NodeId,
    pub(crate) raft: FlareMetaRaft,
    pub(crate) state_machine:
        LocalStateMachineStore<FlareMetadataSM, MetaTypeConfig>,
    raft_config: Arc<Config>,
    flare_config: ServerArgs,
    log_store: MemLogStore<MetaTypeConfig>,
    rpc_service: RaftZrpcService<MetaTypeConfig>,
    node_addr: String,
}

fn resolve_shard_id<'a>(
    meta: &'a CollectionMetadataState,
    key: &[u8],
) -> &'a ShardGroupState {
    let hashed = mur3::murmurhash3_x86_32(key, meta.seed) as u32;
    let shard_count = meta.shards.len();
    let size = u32::div_ceil(u32::MAX, shard_count as u32);
    let shard_index = hashed / size;
    return &meta.shards[shard_index as usize];
}

impl FlareMetadataManager {
    pub async fn new(
        node_id: u64,
        node_addr: String,
        server_args: ServerArgs,
        z_session: Session,
        rpc_prefix: &str,
    ) -> Self {
        let config = Config {
            ..Default::default()
        };
        info!("use raft {:?}", config);
        let config = Arc::new(config.validate().unwrap());
        let log_store = MemLogStore::default();
        let store: LocalStateMachineStore<FlareMetadataSM, MetaTypeConfig> =
            LocalStateMachineStore::default();
        let network = Network::new(z_session.clone(), rpc_prefix.into());
        let resolver = RaftAddrResolver {
            state_machine: store.clone(),
        };
        let resolver = Arc::new(resolver);
        // let client_pool = Arc::new(ClientPool::new(resolver.clone()));
        let control_pool = create_control_pool(resolver.clone());
        let control_pool = Arc::new(control_pool);
        let data_pool = create_data_pool(resolver.clone());
        let data_pool = Arc::new(data_pool);
        let raft = FlareMetaRaft::new(
            node_id,
            config.clone(),
            network,
            log_store.clone(),
            store.clone(),
        )
        .await
        .unwrap();
        let rpc_service = RaftZrpcService::new(
            raft.clone(),
            z_session,
            rpc_prefix.into(),
            node_id,
        );

        FlareMetadataManager {
            raft,
            state_machine: store,
            node_id,
            raft_config: config,
            flare_config: server_args,
            control_pool,
            data_pool,
            log_store,
            node_addr,
            rpc_service,
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

    pub fn create_resolver(&self) -> RaftAddrResolver {
        let resolver = RaftAddrResolver {
            state_machine: self.state_machine.clone(),
        };
        resolver
    }
}

#[async_trait::async_trait]
impl MetadataManager for FlareMetadataManager {
    async fn initialize(&mut self) -> Result<(), FlareError> {
        self.rpc_service.start().await?;
        if self.flare_config.leader {
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
        }

        Ok(())
    }

    async fn get_shard_ids(
        &self,
        col_name: &str,
    ) -> Option<Vec<ShardGroupState>> {
        let state_machine = self.state_machine.clone();
        let col_meta_state = state_machine.state_machine.read().await;
        let col = col_meta_state
            .app_data
            .collections
            .get(col_name)
            .map(|col| col.shards.clone());
        col
    }

    async fn get_shard_id(
        &self,
        col_name: &str,
        key: &[u8],
    ) -> Option<ShardGroupState> {
        let col_meta_state = self.state_machine.state_machine.read().await;
        let col = col_meta_state.app_data.collections.get(col_name);
        if let Some(meta) = col {
            Some(resolve_shard_id(meta, key).clone())
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
            let mut cc = self.control_pool.get(leader_id).await?;
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
            let shard_groups = col
                .shards
                .iter()
                .map(|s| ShardGroup {
                    shard_ids: s.shard_ids.clone(),
                })
                .collect();
            collections.insert(
                name.clone(),
                flare_pb::CollectionMetadata {
                    name: col.name.clone(),
                    shards: shard_groups,
                    replication: col.replication as u32,
                    options: HashMap::new(),
                },
            );
        }
        let mut shards = HashMap::with_capacity(metadata_sm.shards.len());
        let ssm = &metadata_sm.shards;
        for (id, shard) in ssm.iter() {
            shards.insert(*id, shard.into_proto());
        }

        let cm = flare_pb::ClusterMetadata {
            collections,
            shards,
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
        if request.partition_count == 0 {
            return Err(FlareError::InvalidArgument(
                "partition count must be positive".into(),
            ));
        }
        if !self.is_leader().await {
            let leader_id = self.get_leader_id().await?;
            let mut client = self.data_pool.get(leader_id).await?;
            let resp: tonic::Response<CreateCollectionResponse> =
                client.create_collection(request).await?;
            return Ok(resp.into_inner());
        }
        if request.shard_assignments.len() != request.partition_count as usize {
            let sm = self.state_machine.state_machine.read().await;
            let members = sm
                .last_membership
                .nodes()
                .map(|pair| *pair.0)
                .collect::<Vec<u64>>();
            let partition_count = request.partition_count;
            let replica_count = request.replica_count;
            let mut assignments = Vec::new();
            for shard_id in 0..partition_count {
                let primary_index = shard_id as usize % members.len();
                let mut replicas = Vec::with_capacity(replica_count as usize);

                for i in 1..=replica_count {
                    let replica_index =
                        (primary_index + i as usize) % members.len();
                    replicas.push(members[replica_index]);
                }

                assignments.push(ShardAssignment {
                    primary: Some(members[primary_index]),
                    shard_ids: vec![],
                    replica: replicas,
                });
            }
            request.shard_assignments = assignments;
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
pub fn test_resolve_shard() -> Result<(), Box<dyn std::error::Error>> {
    let partition_count = 256;
    let meta = CollectionMetadataState {
        name: "test".into(),
        shards: (0..partition_count)
            .map(|i| ShardGroupState { shard_ids: vec![i] })
            .collect(),
        replication: 1,
        seed: rand::random(),
    };
    for i in 0..1000000 {
        let option = resolve_shard_id(&meta, format!("test-{}", i).as_bytes());
        assert!(option.shard_ids.len() > 0);
    }
    Ok(())
}

pub struct RaftAddrResolver {
    state_machine: LocalStateMachineStore<FlareMetadataSM, MetaTypeConfig>,
}

#[async_trait::async_trait]
impl crate::pool::AddrResolver for RaftAddrResolver {
    async fn resolve(&self, node_id: NodeId) -> Option<String> {
        let sm = self.state_machine.state_machine.read().await;
        if let Some(node) = sm.last_membership.membership().get_node(&node_id) {
            Some(node.addr.clone())
        } else {
            None
        }
    }
}
