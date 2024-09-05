use std::sync::Arc;

use dashmap::DashMap;
use tracing::info;

use crate::cluster::FlareError;

use super::{KvShard, ShardFactory, ShardId, ShardMetadata};

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct HashMapShard {
    pub shard_metadata: ShardMetadata,
    pub map: DashMap<String, Vec<u8>>,
}

#[async_trait::async_trait]
impl KvShard for HashMapShard {
    fn shard_id(&self) -> ShardId {
        self.shard_metadata.id
    }

    async fn get(&self, key: &String) -> Option<Vec<u8>> {
        let out = self.map.get(key);
        out.map(|r| r.clone())
    }

    async fn set(&self, key: String, value: Vec<u8>) -> Result<(), FlareError> {
        self.map.insert(key, value);
        Ok(())
    }

    async fn delete(&self, key: &String) -> Result<(), FlareError> {
        self.map.remove(key);
        Ok(())
    }
}

pub struct HashMapShardFactory {}

impl ShardFactory for HashMapShardFactory {
    fn create_shard(
        &self,
        shard_metadata: ShardMetadata,
    ) -> std::sync::Arc<dyn KvShard> {
        info!("create shard {:?}", &shard_metadata);
        let shard = HashMapShard {
            shard_metadata: shard_metadata,
            ..Default::default()
        };
        let shard = Arc::new(shard);
        shard
    }
}
