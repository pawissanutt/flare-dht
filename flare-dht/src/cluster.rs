use crate::error::FlareError;
use crate::metadata::MetadataManager;
use crate::pool::{ControlPool, DataPool};
use crate::raft::NodeId;
use crate::shard::{KvShard, ShardManager};
use flare_pb::flare_control_client::FlareControlClient;
use flare_pb::JoinRequest;

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
    pub addr: String,
    pub node_id: NodeId,
    pub control_pool: Arc<ControlPool>,
    pub data_pool: Arc<DataPool>,
    pub shard_manager: Arc<ShardManager<T>>,
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
        shard_manager: Arc<ShardManager<T>>,
        control_pool: Arc<ControlPool>,
        data_pool: Arc<DataPool>,
    ) -> Self {
        let (tx, rx) = tokio::sync::watch::channel(false);
        FlareNode {
            metadata_manager: metadata_manager,
            addr,
            node_id,
            shard_manager: shard_manager,
            control_pool,
            data_pool,
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
                        let local_shards =
                            self.metadata_manager.local_shards().await;
                        self.shard_manager.sync_shards(&local_shards);
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

    pub async fn get_shard(
        &self,
        collection: &str,
        key: &[u8],
    ) -> Result<Arc<T>, FlareError> {
        let option = self.metadata_manager.get_shard_id(collection, key).await;
        if let Some(shard_id) = option {
            self.shard_manager.get_shard(shard_id)
        } else {
            Err(FlareError::NoCollection(collection.into()))
        }
    }
}
