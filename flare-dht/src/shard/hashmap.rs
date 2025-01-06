use std::sync::Arc;

use scc::HashMap;
use tracing::info;

use crate::error::FlareError;

use super::{ByteEntry, KvShard, ShardFactory, ShardMetadata};

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct HashMapShard {
    pub shard_metadata: ShardMetadata,
    pub map: HashMap<String, ByteEntry>,
}

#[async_trait::async_trait]
impl KvShard for HashMapShard {
    type Key = String;
    type Entry = ByteEntry;

    fn meta(&self) -> &ShardMetadata {
        &self.shard_metadata
    }

    async fn get(&self, key: &String) -> Result<Option<ByteEntry>, FlareError> {
        let out = self.map.get_async(key).await;
        let out = out.map(|r| r.clone());
        Ok(out)
    }

    // async fn modify<F, O>(&self, key: &Self::Key, f: F) -> Result<O, FlareError>
    // where
    //     F: FnOnce(&mut Self::Entry) -> O + Send,
    // {
    //     let out = match self.map.entry_async(key.clone()).await {
    //         Occupied(mut occupied_entry) => {
    //             let entry = occupied_entry.get_mut();
    //             f(entry)
    //         }
    //         Vacant(vacant_entry) => {
    //             let mut entry = Self::Entry::default();
    //             let o = f(&mut entry);
    //             vacant_entry.insert_entry(entry);
    //             o
    //         }
    //     };
    //     Ok(out)
    // }

    async fn set(
        &self,
        key: String,
        value: ByteEntry,
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
