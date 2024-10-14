use crate::error::FlareError;
use crate::metadata::MetadataManager;
use crate::pool::ClientPool;
use crate::raft::NodeId;
use crate::shard::{KvShard, ShardFactory, ShardId};
use flare_pb::flare_control_client::FlareControlClient;
use flare_pb::JoinRequest;

use scc::HashMap;
use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Uri};
use tracing::info;

pub struct FlareNode<T>
where
    T: KvShard,
{
    pub metadata_manager: Arc<dyn MetadataManager>,
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
        metadata_manager: Arc<dyn MetadataManager>,
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
            self.metadata_manager.create_watch(),
        );
        tokio::spawn(async move {
            let mut last_sync = 0;
            loop {
                if let Some(log_id) = rs.next().await {
                    if log_id > last_sync {
                        last_sync = log_id;
                        self.sync_shard().await;
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

    pub async fn leave(&self) {
        self.metadata_manager.leave().await;
        info!("flare leave group");
    }

    pub async fn close(&self) {
        self.leave().await;
        self.close_signal_sender.send(true).unwrap();
    }

    pub async fn sync_shard(&self) {
        tracing::debug!("sync shard");
        let local_shards = self.metadata_manager.local_shards().await;
        for s in local_shards {
            if self.shards.contains(&s.id) {
                continue;
            }
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
}
