use crate::error::FlareError;
use crate::metadata::state_machine::{
    FlareControlRequest, FlareControlResponse,
};
use crate::metadata::FlareMetadataManager;
use crate::pool::ClientPool;
use crate::raft::NodeId;
use crate::shard::{KvShard, ShardFactory, ShardId};
use flare_pb::flare_control_client::FlareControlClient;
use flare_pb::{
    CreateCollectionRequest, CreateCollectionResponse, JoinRequest,
    LeaveRequest,
};

use openraft::ChangeMembers;
use scc::HashMap;
use std::collections::BTreeMap;
use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Uri};
use tracing::info;

pub struct FlareNode<T>
where
    T: KvShard,
{
    pub metadata_manager: Arc<FlareMetadataManager>,
    pub shards: HashMap<ShardId, Arc<T>>,
    pub addr: String,
    pub node_id: NodeId,
    pub shard_factory: Box<dyn ShardFactory<T>>,
    pub client_pool: Arc<ClientPool>,
    close_signal_sender: tokio::sync::watch::Sender<bool>,
    close_signal_receiver: tokio::sync::watch::Receiver<bool>,
}

impl<T> FlareNode<T>
where
    T: KvShard + 'static,
{
    pub async fn new(
        addr: String,
        node_id: NodeId,
        metadata_manager: Arc<FlareMetadataManager>,
        shard_factory: Box<dyn ShardFactory<T>>,
        client_pool: Arc<ClientPool>,
    ) -> Self {
        let shards = HashMap::new();
        let (tx, rx) = tokio::sync::watch::channel(false);
        FlareNode {
            shards: shards,
            metadata_manager: metadata_manager,
            addr,
            node_id,
            shard_factory: shard_factory,
            client_pool,
            close_signal_sender: tx,
            close_signal_receiver: rx,
        }
    }

    pub fn start_watch_stream(self: Arc<Self>) {
        let mut rs = tokio_stream::wrappers::WatchStream::new(
            self.metadata_manager.raft.data_metrics(),
        );
        tokio::spawn(async move {
            let mut last_sync = 0;
            loop {
                if let Some(d) = rs.next().await {
                    if let Some(log_id) = d.last_applied {
                        if log_id.index > last_sync {
                            last_sync = log_id.index;
                            self.sync_shard().await;
                        }
                    }
                    if self.close_signal_receiver.has_changed().unwrap_or(true)
                    {
                        info!("closed watch loop");
                        break;
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

    // async fn shutdown(&self) {
    //     self.metadata_manager.raft.shutdown().await.unwrap();
    // }

    pub async fn leave(&self) {
        let mm = self.metadata_manager.clone();
        let mut nodes = std::collections::BTreeSet::new();
        nodes.insert(self.node_id);
        if mm.is_leader().await {
            let sm = mm.state_machine.state_machine.read().await;
            let node_count = sm.last_membership.nodes().count();
            drop(sm);
            if node_count == 1 {
                // self.shutdown().await;
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

    pub async fn close(&self) {
        self.leave().await;
        self.close_signal_sender.send(true).unwrap();
    }

    pub async fn sync_shard(&self) {
        tracing::debug!("sync shard");
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
            .filter(|shard| !self.shards.contains(&shard.id));
        for s in local_shards {
            let shard = self.shard_factory.create_shard(s.clone());
            let shard_id = shard.meta().id;
            self.shards.upsert(shard_id, shard);
        }
    }

    pub async fn get_shard(
        &self,
        collection: &str,
        key: &str,
    ) -> Result<Arc<T>, FlareError> {
        let option = self.metadata_manager.get_shard_id(collection, key).await;
        if let Some(shard_id) = option {
            self.shards
                .get(&shard_id)
                .map(|shard| shard.get().clone())
                .ok_or_else(|| FlareError::NoShardFound(shard_id))
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
        if !self.metadata_manager.is_leader().await {
            let leader_id = self.metadata_manager.get_leader_id().await?;
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
