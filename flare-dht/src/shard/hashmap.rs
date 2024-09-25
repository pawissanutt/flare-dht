use std::sync::Arc;

use scc::HashMap;
use tracing::info;

use crate::error::FlareError;

use super::{KvShard, ShardEntry, ShardFactory, ShardMetadata};

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct HashMapShard {
    pub shard_metadata: ShardMetadata,
    pub map: HashMap<String, ShardEntry>,
}

#[async_trait::async_trait]
impl KvShard for HashMapShard {
    type Entry = ShardEntry;

    fn meta(&self) -> &ShardMetadata {
        &self.shard_metadata
    }

    async fn get(
        &self,
        key: &String,
    ) -> Result<Option<ShardEntry>, FlareError> {
        let out = self.map.get_async(key).await;
        let out = out.map(|r| r.clone());
        Ok(out)
    }

    async fn set(
        &self,
        key: String,
        value: ShardEntry,
    ) -> Result<(), FlareError> {
        self.map.upsert_async(key, value).await;
        Ok(())
    }

    async fn delete(&self, key: &String) -> Result<(), FlareError> {
        self.map.remove_async(key).await;
        Ok(())
    }
}

pub struct HashMapShardFactory {}

impl ShardFactory<HashMapShard> for HashMapShardFactory {
    fn create_shard(
        &self,
        shard_metadata: ShardMetadata,
    ) -> std::sync::Arc<HashMapShard> {
        info!("create shard {:?}", &shard_metadata);
        let shard = HashMapShard {
            shard_metadata: shard_metadata,
            ..Default::default()
        };
        let shard = Arc::new(shard);
        shard
    }
}
