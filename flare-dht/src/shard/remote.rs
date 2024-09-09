use flare_pb::{flare_kv_client::FlareKvClient, SingleKeyRequest};
use tonic::transport::Channel;

use crate::error::FlareError;

use super::{KvShard, ShardEntry, ShardMetadata};

#[derive(Clone, Debug)]
struct RemoteShard {
    shard_metadata: ShardMetadata,
    client: Box<FlareKvClient<Channel>>
}

#[async_trait::async_trait]
impl KvShard for RemoteShard {
    
    fn meta(&self) -> &ShardMetadata {
        &self.shard_metadata
    }

    async fn get(&self, key: &String) -> Option<ShardEntry> {
        // self.client.get(SingleKeyRequest{
        //     collection: self.shard_metadata.collection.clone(),
        //     key: key.into(),
        // }).await;
        todo!()
    }

    async fn set(&self, key: String, value: ShardEntry) -> Result<(), FlareError> {
        todo!()
    }

    async fn delete(&self, key: &String) -> Result<(), FlareError> {
        todo!()
    }
}
