use crate::error::{FlareError, FlareInternalError};
use crate::metadata::state_machine::{
    FlareControlRequest, FlareControlResponse,
};
use crate::metadata::FlareMetadataManager;
use crate::pool::ClientPool;
use crate::raft::NodeId;
use crate::shard::HashMapShardFactory;
use crate::shard::{KvShard, ShardFactory, ShardId};
use crate::ServerArgs;
use dashmap::DashMap;
use flare_pb::flare_control_client::FlareControlClient;
use flare_pb::flare_kv_client::FlareKvClient;
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
use tonic::{client, Status};
use tracing::info;

#[derive(Clone)]
pub struct FlareNode {
    pub metadata_manager: Arc<FlareMetadataManager>,
    pub shards: Arc<DashMap<ShardId, Arc<dyn KvShard>>>,
    pub addr: String,
    pub node_id: NodeId,
    pub shard_factory: Arc<dyn ShardFactory>,
    pub client_pool: ClientPool,
    close_signal_sender: tokio::sync::watch::Sender<bool>,
    close_signal_receiver: tokio::sync::watch::Receiver<bool>,
}



impl FlareNode {
    // #[tracing::instrument]
    pub async fn new(options: ServerArgs) -> Self {
        let shards = DashMap::new();
        let node_id = options.get_node_id();
        info!("use node_id: {node_id}");
        let addr = options.get_addr();
        let mm = Arc::new(FlareMetadataManager::new(node_id).await);
        let pool = ClientPool::new(mm.clone());
        let (tx, rx) = tokio::sync::watch::channel(false);
        FlareNode {
            shards: Arc::new(shards),
            metadata_manager: mm,
            addr,
            node_id,
            shard_factory: Arc::new(HashMapShardFactory {}),
            client_pool: pool,
            close_signal_sender: tx,
            close_signal_receiver: rx,
        }
    }

    pub fn start_watch_stream(self: Arc<Self>) {
        let mut rs = tokio_stream::wrappers::WatchStream::new(
            self.metadata_manager.raft.data_metrics(),
        );
        let mut close_rs = tokio_stream::wrappers::WatchStream::new(self.close_signal_receiver.clone());
        tokio::spawn(async move {
            let mut last_sync = 0;
            loop {
                // tokio::select! {
                //     Some(d) = rs.next() => {
                //         info!("event!!!");
                //         if let Some(la) = d.last_applied {
                //             if la.index > last_sync {
                //                 last_sync = la.index;
                //                 self.sync_shard().await;
                //             }
                //         }
                //     },
                //     // Some(_) = close_rs.next() => break,
                //     else => break,
                // }
                if let Some(d) = rs.next().await {
                    if let Some(la) = d.last_applied {
                        if la.index > last_sync {
                            last_sync = la.index;
                            self.sync_shard().await;
                        }
                    }
                    if self.close_signal_receiver.has_changed().unwrap_or(true){
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
            .filter(|shard| !self.shards.contains_key(&shard.id));
        for s in local_shards {
            let shard = self.shard_factory.create_shard(s.clone());
            self.shards.insert(shard.meta().id, shard);
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
        if !self.metadata_manager.is_leader().await {
            let leader_channel = self.client_pool.get_leader().await?;
            let mut client = FlareKvClient::new(leader_channel);
            let resp = client.create_collection(request).await?;;
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
