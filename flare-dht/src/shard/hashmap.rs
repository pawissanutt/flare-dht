use std::sync::Arc;

use dashmap::DashMap;
use tracing::info;

use crate::error::FlareError;

use super::{KvShard, ShardEntry, ShardFactory, ShardMetadata};

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct HashMapShard {
    pub shard_metadata: ShardMetadata,
    pub map: DashMap<String, ShardEntry>,
}

#[async_trait::async_trait]
impl KvShard for HashMapShard {
    fn meta(&self) -> &ShardMetadata {
        &self.shard_metadata
    }

    async fn get(&self, key: &String) -> Option<ShardEntry> {
        let out = self.map.get(key);
        out.map(|r| r.clone())
    }

    async fn set(
        &self,
        key: String,
        value: ShardEntry,
    ) -> Result<(), FlareError> {
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
