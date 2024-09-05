use crate::metadata::state_machine::{
    FlareControlRequest, FlareControlResponse,
};
use crate::metadata::FlareMetadataManager;
use crate::shard::hashmap::HashMapShardFactory;
use crate::shard::{KvShard, ShardFactory, ShardId};
use crate::{raft::NodeId, FlareOptions};
use dashmap::DashMap;
use flare_pb::flare_control_client::FlareControlClient;
use flare_pb::{
    CreateCollectionRequest, CreateCollectionResponse, JoinRequest,
    LeaveRequest,
};
use openraft::ChangeMembers;
use std::collections::BTreeMap;
use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Uri};
use tonic::Status;
use tracing::info;

#[derive(Clone)]
pub struct FlareNode {
    pub metadata_manager: Arc<FlareMetadataManager>,
    pub shards: Arc<DashMap<ShardId, Arc<dyn KvShard>>>,
    pub addr: String,
    pub node_id: NodeId,
    pub shard_factory: Arc<dyn ShardFactory>,
}

#[derive(thiserror::Error, Debug)]
pub enum FlareError {
    #[error("No shard `{0}` on current node")]
    NoShardFound(ShardId),
    #[error("No collection `{0}` in cluster")]
    NoCollection(String),
    #[error("Invalid: {0}")]
    InvalidArgument(String),
    #[error("Invalid: {0}")]
    UnknownError(#[from] Box<dyn Error + Send + Sync + 'static>),
}

impl From<FlareError> for tonic::Status {
    fn from(value: FlareError) -> Self {
        match value {
            FlareError::InvalidArgument(msg) => Status::invalid_argument(msg),
            FlareError::UnknownError(e) => Status::from_error(e),
            _ => Status::not_found(value.to_string()),
        }
    }
}

impl FlareNode {
    // #[tracing::instrument]
    pub async fn new(options: FlareOptions) -> Self {
        let shards = DashMap::new();
        let node_id = options.get_node_id();
        info!("use node_id: {node_id}");
        let addr = options.get_addr();
        let mm = Arc::new(FlareMetadataManager::new(node_id).await);
        FlareNode {
            shards: Arc::new(shards),
            metadata_manager: mm,
            addr,
            node_id,
            shard_factory: Arc::new(HashMapShardFactory {}),
        }
    }

    pub fn start_watch_stream(self: Arc<Self>) {
        let mut rs = tokio_stream::wrappers::WatchStream::new(
            self.metadata_manager.raft.data_metrics(),
        );
        tokio::spawn(async move {
            let mut last_sync = 0;
            while let Some(d) = rs.next().await {
                if let Some(la) = d.last_applied {
                    if la.index > last_sync {
                        last_sync = la.index;
                        self.sync_shard().await;
                    }
                }
            }
        });
    }

    pub async fn init_leader(&self) -> Result<(), Box<dyn Error>> {
        let mm = self.metadata_manager.clone();
        let mut map = BTreeMap::new();
        map.insert(
            self.node_id,
            openraft::BasicNode {
                addr: self.addr.clone(),
            },
        );
        mm.raft.initialize(map).await?;
        Ok(())
    }

    pub async fn join(&self, peer_addr: &str) -> Result<(), Box<dyn Error>> {
        info!("advertise addr {}", self.addr);
        let peer_addr: Uri = Uri::from_str(peer_addr)?;
        let channel = Channel::builder(peer_addr).connect_lazy();
        let mut client = FlareControlClient::new(channel);
        let resp = client
            .join(JoinRequest {
                node_id: self.node_id,
                addr: self.addr.clone(),
            })
            .await?;
        resp.into_inner();
        Ok(())
    }

    async fn shutdown(&self) {
        self.metadata_manager.raft.shutdown().await.unwrap();
    }

    pub async fn leave(&self) {
        let mm = self.metadata_manager.clone();
        let mut nodes = std::collections::BTreeSet::new();
        nodes.insert(self.node_id);
        if mm.is_leader().await {
            let sm = mm.state_machine.state_machine.read().await;
            let node_count = sm.last_membership.nodes().count();
            drop(sm);
            if node_count == 1 {
                self.shutdown().await;
                return;
            } else {
                let change_members: ChangeMembers<u64, openraft::BasicNode> =
                    ChangeMembers::RemoveVoters(nodes);
                mm.raft
                    .change_membership(change_members, true)
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_secs(2)).await;
                match mm.create_control_client().await {
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
            let is_voter = mm.is_current_voter().await;
            let mut client = mm.create_control_client().await.unwrap();
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
        info!("flare leave group");
    }

    pub async fn sync_shard(&self) {
        info!("sync shard");
        let sm = self
            .metadata_manager
            .state_machine
            .state_machine
            .read()
            .await;
        let local_shards = sm
            .app_data
            .shards
            .values()
            .filter(|shard| shard.primary.unwrap_or(0) == self.node_id)
            .filter(|shard| !self.shards.contains_key(&shard.id));
        for s in local_shards {
            let shard = self.shard_factory.create_shard(s.clone());
            self.shards.insert(shard.shard_id(), shard);
        }
    }

    pub async fn get_shard(
        &self,
        collection: &str,
        key: &str,
    ) -> Result<Arc<dyn KvShard>, FlareError> {
        let option = self.metadata_manager.get_shard_id(collection, key).await;
        if let Some(shard_id) = option {
            if let Some(shard) = self.shards.get(&shard_id) {
                Ok(shard.clone())
            } else {
                Err(FlareError::NoShardFound(shard_id))
            }
        } else {
            Err(FlareError::NoCollection(collection.into()))
        }
    }

    pub async fn create_collection(
        &self,
        mut request: CreateCollectionRequest,
    ) -> Result<CreateCollectionResponse, FlareError> {
        if request.shard_count == 0 {
            return Err(FlareError::InvalidArgument(
                "shard count must be positive".into(),
            ));
        }
        if request.shard_assignments.len() != request.shard_count as usize {
            let node_id = self.node_id;
            request.shard_assignments =
                vec![node_id].repeat(request.shard_count as usize);
        }
        let req = FlareControlRequest::CreateCollection(request);
        let resp = self
            .metadata_manager
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
}
